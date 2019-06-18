/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backendtest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.api.java.utils.ParameterTool;



import org.apache.flink.api.java.functions.KeySelector;

import java.util.*;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;

import java.util.concurrent.TimeUnit;



/**
 * <p> --------------------Intellinse Backend Test Project-----------------------------
 * <p>Sensory data anomaly detection, based on a streaming data pipeline processing.
 * <p>The pipeline reads data from the provided file, performs processing to allocate an anomalous score, 
 * <p>and then write the data  and corresponding anomaly scores into InfluxDB.
 *
 *
 * <p>Usage: <code>BackendTest --input &lt;path&gt; --dbname &lt;path&gt; --window &lt;n&gt; --method &lt;n&gt;</code><br>
 *
 *
 *
 * <p>The InfluxDB input entries have the following "schema":
 * <p> name: measurements
 * <p> time               | sensor ID (tag)| anomaly score (tag) | value of sensor entry (field) |
 * <p> -------------------|----------------|---------------------|-------------------------------|
 * <p> 2017-01-01T00:00:00Z|       1        |        0.5  		 |       215.304292299079        |
 * 
 * <p> This is the main class for execution.
 *  */

public class StreamingJob {
		
    public static int main(String[] args) throws Exception {
    	
    	final ParameterTool params = ParameterTool.fromArgs(args);

		// get the input file
		String dataSource ="";
		if (params.has("input")) {
			// read the text file from given input path
			dataSource = params.get("input");
		} else {
			System.err.println("Undefined input file. Use --input to specify data source. Application now exiting");
			return -1;

		}

		//get the database name
		String dbname="";
		if (params.has("dbname")) {
			// read the text file from given input path
			dbname = params.get("dbname");
		} else {
			System.err.println("Undefined database. Use --dbname to specify data sink. Application now exiting");
			return -1;
		}		


		//the size of the window for performing anomaly detection.
		int windowSize =100;
		if (params.has("window")) {
			windowSize = params.getInt("window");
		}
		

		//the method for anomaly detection (default method: provided by test description).
		String adMethod="default";
		if (params.has("method")) {
			adMethod = params.get("method");	
		}
		if (!(adMethod.equals("default")) && !(adMethod.equals("tukey"))){
			System.err.println("Unsupported method for anomaly detection. Compatible methods are: default and tukey. Application now exiting");
			return -1;
		}	

		

		//parse the input file
		DataCreator inputfile = new DataCreator(dataSource);
		
		ArrayList<SensorEntry> tmp = inputfile.getCollection();
		if (tmp ==null) {
			
			return -1;
		}		
		
		// set up the streaming execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		see.getConfig().setGlobalJobParameters(params); 
		
		/*InfluxDB Configuration (Considers an already existing database. 
		 *(A database with input name defined in the input parameters should have already been created)
		 */
		  InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://localhost:8086", "root", "root", dbname)
		      .batchActions(1000)
		      .flushDuration(100, TimeUnit.MILLISECONDS)
		      .enableGzip(true)
		      .build();

		  
		
		//Create the initial datastream from SensorEntry elements.
		DataStream<SensorEntry> datain = see.fromCollection(tmp);
		
				
		
		
		//Transformation to a keyed stream w.r.t. to the sensor ID.
		KeyedStream<SensorEntry, Integer> keyedData= datain
				.keyBy(new KeySelector<SensorEntry, Integer>(){
				@Override
				public Integer getKey(SensorEntry sensorData) {
				
				return sensorData.getSensorId();
			        }
		});
		
	         
	    //Split into non-overlapping windows per sensor ID of $N$ 
		//elements and calculation of the anomaly score for each sensor value.  
		DataStream<List<SensorEntry>> processedData = keyedData
			      .window(GlobalWindows.create())
			      .trigger(PurgingTrigger.of(CountTrigger.of(windowSize)))
			      .process(new AnomalyDetector(adMethod));
	      
			
    	//Unfold the windowed data into the output datastream, containing the anomaly score for each sensor entry.
		DataStream<SensorEntry> outputData = processedData
				.flatMap(new FlatMapFunction<List<SensorEntry>, SensorEntry>() {
				@Override
				public void flatMap(List<SensorEntry> input, Collector<SensorEntry> out)
				throws Exception{
				for (SensorEntry s: input) {
					
					out.collect(s);
				}	
					
				}	
				});
		
    	
		//Create the InfluxDB input entries with the following "schema":
		DataStream<InfluxDBPoint> dbStream = outputData.map(
		                new RichMapFunction<SensorEntry, InfluxDBPoint>() {
		                    @Override
		                    public InfluxDBPoint map(SensorEntry s) throws Exception {
		                  
		                    	String measurement = "measurements";
		                        long timestamp = s.getTimestamp(); 

		                        HashMap<String, String> tags = new HashMap<>();
		                        tags.put("sensorId", String.valueOf(s.getSensorId()));
		                        tags.put("anomalyScore", String.valueOf(s.getAnomalyScore()));

		                        HashMap<String, Object> fields = new HashMap<>();
		                        fields.put("value", s.getValue());
		                        

		                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
		                    }
		                }
		        );

    	  //connect the sink to the final stream for writing results to InfluxDB.
		  dbStream.addSink(new InfluxDBSink(influxDBConfig));
		
		  // execute program
		  see.execute("Intellisense Backend Test");
				
		  return 1;
    }

		
	
}

