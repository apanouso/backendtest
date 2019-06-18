package backendtest;

import java.util.*;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.apache.flink.api.java.tuple.Tuple2;

/** 
 * <p>--------------------Intellinse Backend Test Project-----------------------------
 * <p>Sensory data anomaly detection, based on a streaming data pipeline processing.
 * <p>The pipeline reads data from the provided file, performs processing to allocate an anomalous score, 
 * <p>and then write the data  and corresponding anomaly scores into InfluxDB.
 * <p>
 * <p>
 * <p>This class performs the anomaly detection over windows of keyed streaming data (observation window).
 * <p> Data are grouped using the sensor id as the key.
 * <p> The process function returns the list of sensor entries over which the anomaly score has been calculated.  
 *  
 *  */

 
public class AnomalyDetector 
extends ProcessWindowFunction<SensorEntry, List<SensorEntry>, Integer, GlobalWindow> {

public String adMethod="";

public AnomalyDetector(String adMethod){

this.adMethod = adMethod;

}

@Override

/** The anomaly score calculation function.
 * 
 * @param key  the key for grouping the data.
 * @param context the window context
 * @param input the input entries corresponding to the observation window.
 * @return out the list of sensor entries with the calculated anomaly score.
 */

public void process(Integer key, Context context, Iterable<SensorEntry> input, Collector<List<SensorEntry>> out) {

	DescriptiveStatistics da = new DescriptiveStatistics();

	int c=0;
	int sensorid=-1;
	//calculate the IQR over the observation window.
	for (SensorEntry in: input) {
		da.addValue(in.getValue());
	}

	double iqr = da.getPercentile(75.) - da.getPercentile(25.);
    //perform the anomaly score calculation.	
	List<SensorEntry> processedIn = new ArrayList<SensorEntry>();
	for (SensorEntry in: input) {
		if (this.adMethod.equals("tukey")){
		processedIn.add(new SensorEntry(in.getTimestamp(),in.getSensorId(),in.getValue(), myUtils.calcIQRAnomalyScoreTukey(in.getValue(),iqr,da.getPercentile(25.), da.getPercentile(75.), 1.5, 1.5)));
		} else if (this.adMethod.equals("default")){
	processedIn.add(new SensorEntry(in.getTimestamp(),in.getSensorId(),in.getValue(), myUtils.calcIQRAnomalyScore(in.getValue(), iqr,1.5, 3)));

}     else {
		System.err.println("Unsupported method. You should not be in here!");
		break;
	}
	
	}
    //return the processed entries.
	out.collect(processedIn);
	}

}
