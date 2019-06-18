package backendtest;


/** 
 * <p>--------------------Intellinse Backend Test Project-----------------------------
 * <p>Sensory data anomaly detection, based on a streaming data pipeline processing.
 * <p>The pipeline reads data from the provided file, performs processing to allocate an anomalous score, 
 * <p>and then write the data  and corresponding anomaly scores into InfluxDB.
 * <p>
 * <p>
 * <p>This class represents the sensor entries to be processed. Each sensor entry is characterised by:
 * <p>--the sensor id
 * <p>--the timestamp of sampling
 * <p>--the sampled value
 * <p>--the anomaly score (initial value = -1). 
 *  
 *  */

public class SensorEntry{
	
	private final long timestamp;
	private final int sensorid;
	private final double sensorval;
	private double anomalyScore;
	
	
	/**the class constructor.
	 * @param timestamp	the timestamp of sampling (in Java Timestamp format)
	 * @param sensorid	the id of the sensor
	 * @param sensorval the measurement
	 * @param the anomaly score for this measurement.
	*/
	public SensorEntry(long timestamp, int sensorid, double sensorval, double score) {
		
		this.timestamp = timestamp;
		this.sensorid = sensorid;
		this.sensorval = sensorval;
		this.anomalyScore = score;
		
	}
	
	/** Public access to the sampling timestamp.
	 * 
	 * @return the sampling timestamp 
	 * 
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/** Public access to the sensor id. 
	 * 
	 * @return the sensor id 
	 * 
	 */
	public int getSensorId() {
		return sensorid;
	}
	
	
	/** Public access to the sampling value. 
	 * 
	 * @return the sampling value.
	 */
	public double getValue() {
		return sensorval;
	}
	
	
	/** Public access to the anomaly score. 
	 * 
	 * @return the anomaly score. 
	 * 
	 */
	public double getAnomalyScore() {
		return anomalyScore;
	}

	
}
