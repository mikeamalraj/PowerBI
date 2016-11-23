package etl.core;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.log4j.Logger;

public class FileChecker {
	
	public static Logger logger;
	static
	{
		logger = Logger.getLogger(FileChecker.class);
	}
	
	/** The moveFile moves the processed file into sub (Processed) folder. */
	public boolean moveFile(String fileDir, String fileName) {
		File afile = new File(fileDir + "/" + fileName);
		String[] fileInfo = fileName.split("/.");
		SimpleDateFormat date = new SimpleDateFormat("_yyyyMMddHHmmss");
		String newName = fileDir + "Processed/" + fileInfo[0] + date.format(new Date()) + ".txt";
		System.out.println("New Processed File Location : " + newName);
		if (afile.renameTo(new File(newName))) {
			return true;
		}
		return false;
	}
	
	public int checkFileProcessedStatus(String resourceFileLocation, String fileName) 
	{
		Map<String, String> propmap = new HashMap<String, String>();
	   Properties properties = new Properties();
	   try {
			 InputStream inputStream = new FileInputStream(new File(resourceFileLocation + "Filedetails.properties"));
			 properties.load(inputStream);
			
			for (String key : properties.stringPropertyNames()) {
				propmap.put(key, properties.getProperty(key));
			}
		}
		catch(Exception e)
		{
			logger.fatal( fileName + " : Filedetails.properties file load failed!");
			return 3;
		}
       //key exists
	   if (propmap.containsKey(fileName) && propmap.get(fileName).toString().contains("Completed")) {
           logger.info( fileName + " file : already Processed. Hence, processing is skipped now!");
           return 0;
       } else {
           //key does not exists
    	   System.out.println(fileName + " File needs to be Processed");
           return 1;
       }
	}
	
	public void WritePropertiesFile(String resourceFileLocation, String fileName) {
        FileOutputStream fileOut = null;
        FileInputStream fileIn = null;
        try {
            Properties configProperty = new Properties();
            
            File file = new File(resourceFileLocation + "Filedetails.properties");
            fileIn = new FileInputStream(file);
            configProperty.load(fileIn);
            configProperty.setProperty(fileName, "Completed");
            fileOut = new FileOutputStream(file);
            configProperty.store(fileOut, "");

        } catch (Exception ex) {
        	logger.error( fileName + " : Filedetails.properties write failed!");
        } finally {
            try {
                fileOut.close();
            } catch (IOException ex) {
            	logger.info( fileName + " : Exception while closing Filedetails.properties.");
            }
        }
    }
	
	public String getLatestDailyFile(String resourceFileLocation, String taskName ) throws Exception
	{
		final String DailyTaskName=taskName;
		String LatestFile;
		File directory = new File(resourceFileLocation);
		String[] fileNames = directory.list(new FilenameFilter() {
		    public boolean accept(File dir, String fileName) {
		         return fileName.contains(DailyTaskName);
		    }
		});
		if (fileNames.length>0)
		{
			Arrays.sort(fileNames);
			LatestFile = fileNames[fileNames.length-1];
			System.out.println("Latest file name is: " + LatestFile);
			System.out.println(fileNames.length);
			return LatestFile;
		}
		else
		{
			return null;
		}
	}
	
	
}
