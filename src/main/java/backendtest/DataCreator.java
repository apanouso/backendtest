package backendtest;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;


import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import java.sql.Timestamp;

/** 
 * <p>--------------------Intellinse Backend Test Project-----------------------------
 * <p>Sensory data anomaly detection, based on a streaming data pipeline processing.
 * <p>The pipeline reads data from the provided file, performs processing to allocate an anomalous score, 
 * <p>and then write the data  and corresponding anomaly scores into InfluxDB.
 * <p>
 * <p>
 * <p>The class responsible for converting the contents of the input CSV file into a collection of SensorEntries.
 *  
 *  */


public class DataCreator {

	private final String csvfile;
	private final ArrayList<SensorEntry> dataCollection;
	
	
	/** The class constructor
	 * 
	 * @param csvfile the path to the csvfile. 
	 * 
	 **/
	public DataCreator(String csvfile) {
		
		this.csvfile = csvfile;
		this.dataCollection = new ArrayList<SensorEntry>();
		
		
		
	}	
	
	/**Reads the csv file and converts its lines into a list of sensor entries (represented as SensorEntry objects)
	 * The format of the csv file consered is:
	 * Date, Sensor ID#1, Sensor ID#2, ....Sensor ID#N \n
	 * 
	 * @param csvfile the path to the csv file.
	 * @return TRUE of the list has been successfully generated, FALSE if the file path was problematic.
	 * 
	 * **/
	private boolean createCollection(String csvfile) {
		
		
		CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(csvfile));
            String[] line;
            line = reader.readNext(); //first line is data description.
            while ( (line= reader.readNext())!=null) {
				//first element is timestamp
            	//broken entry
            	if (line.length<=1) {
            		//move to next line....
					System.err.println("Empty time entry found. Moving on to next line...");
            		continue;
            	}
            	Timestamp logTimestamp= myUtils.convertStringToTimestamp(line[0]);
				
            	if (logTimestamp == null) {
            		//move to next line....
					System.err.println("Empty time entry found. Moving on to next line...");
            		continue;
            	}
            	long timestamp = logTimestamp.getTime();
				//remaining are sensor readings. 
				Double value = 0.;
				
				for (Integer i=1;i<line.length;i++) {
					try {
					this.dataCollection.add(new SensorEntry(timestamp, i, value.valueOf(line[i]), -1));
					}
					catch (NumberFormatException e) {
						//move to next line....
						System.err.println("Empty sensor entry found. Moving on to next sensor...");
						continue;
					}
				}
	            

                }
        	reader.close();  
        } catch (IOException e) {
        	System.err.println("Cannot read the input file. Something wrong with file path?");
        	return false;
            
        }
        
        
        return true;
      
    }


	
	/**Calculates and yields the collection of sensor entries (represented as SensorEntry objects)
	 * 
	 * @return the collection of sensor entries.
	 * 
	 * **/	
	
	public ArrayList<SensorEntry> getCollection(){
		if 	(!createCollection(this.csvfile)) {
			
			return null;
		}
		return this.dataCollection;
		
	}
	}
    

    
        
