package backendtest;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.ArrayList;


/** 
 * <p>--------------------Intellinse Backend Test Project-----------------------------
 * <p>Sensory data anomaly detection, based on a streaming data pipeline processing.
 * <p>The pipeline reads data from the provided file, performs processing to allocate an anomalous score, 
 * <p>and then write the data  and corresponding anomaly scores into InfluxDB.
 * <p>
 * <p>
 * <p>Auxiliary class for useful calculations, including the IRQ-based anomaly detection algorithms.
 *  
 *  */

public class myUtils{
	
 

	
   
 /** Converts a timestamp in the format yyyy-MM-ddThh:mm:ssZ to Java timestamp 
   * 
   * @param str_date the input date in string format.
   * @return the resulting Java timestamp
   * 
   * */	

  public static Timestamp convertStringToTimestamp(String str_date) {
    try {
      DateFormat formatter;
      formatter = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
       // you can change format of date
      
      Date date = formatter.parse(str_date);
      Timestamp timeStampDate = new Timestamp(date.getTime());

      return timeStampDate;
    } catch (ParseException e) {
      System.out.println("Exception :" + e);
      return null;
    }
  }
  
  
 
  
  /** Converts an ArrayList to String using the ',' delimiter.  
   * 
   * @param inputlist the ArrayList to be converted. 
   * @return the resulting string representation.
   * 
   * */
  public static String convertToString(ArrayList<Double> inputlist) {
	  
	  if (inputlist == null) {
		  return "";
		  
	  }
      StringBuilder builder = new StringBuilder();
      // Append all Integers in StringBuilder to the StringBuilder.
      for (double value : inputlist) {
          builder.append(value);
          builder.append(",");
      }
      // Remove last delimiter with setLength
      builder.setLength(builder.length() - 1);
      return builder.toString();
  }
  
 
  
  /** The IQR-based calculation of the anomaly score, that considers a lower (minval) and upper (maxval) threshold. 
   * The algorithm is:
   * 
   * IF value < IQR*minval THEN anomaly score = 0
   * 
   * ELSE IF IQR*minval <=value <IQR*maxval THEN anomaly score = 0.5
   * 
   *ELSE IF value >= IQR*maxval THEN anomaly score = 1 
   * 
   * @param value the sampling measurement.
   * @param iqr the interquartile range
   * @param minval the lower threshold
   * @param maxval the upper threshold. 
   * @return the anomaly score. 
   * 
   * */
  public static double calcIQRAnomalyScore(double value, double iqr, double minval, double maxval) {
	  
	  if (value < iqr*minval) {
		  return 0;
	  }
	  else if ((value >=iqr*minval) && (value <iqr*maxval)) {
		 return 0.5;
	  }
	  else if (value>=iqr*maxval) {
		  return 1;
		  
	  }
	  return -1;
	  
}
  

  /** The calculation of the anomaly score, using Tukey's algorithm.  
   * The algorithm is:
   * 
   * IF value < Q1 - IQR*minval THEN anomaly score = 0
   * 
   * ELSE IF Q1 - IQR*minval <=value < Q3 + IQR*maxval THEN anomaly score = 0.5
   * 
   *ELSE IF value >= Q3 + IQR*maxval THEN anomaly score = 1 
   * 
   * @param value the sampling measurement.
   * @param iqr the interquartile range
   * @param q1 the 25th quartile
   * @param q3 the 75th quartile 
   * @param minval the lower threshold
   * @param maxval the upper threshold. 
   * @return the anomaly score. 
   * 
   * */  
  public static double calcIQRAnomalyScoreTukey(double value, double iqr, double q1, double q3, double minval, double maxval) {
	  if (value < q1- iqr*minval) {
		  return 0;
	  }
	  else if ((value >=q1 - iqr*minval) && (value <q3 + iqr*maxval)) {
		 return 0.5;
	  }
	  else if (value>=q3 + iqr*maxval) {
		  return 1;
		  
	  }
	  return -1;
	  
}
}
