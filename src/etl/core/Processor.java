package etl.core;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import etl.config.Configurations;
import etl.config.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Processor {
	/** The Logger object is initialized here(in order to create log file for Processor class) based on Processor class .*/  
	static Logger logger = Logger.getLogger(Processor.class);
	FileChecker fc = new FileChecker();
	
	/** The main method gets the command line arguments informations. If all needed informations are passed then it will call execute method for processing. Otherwise it will call message method */ 
	public static void main(String args[]) throws Exception {
		
		/** If no argument is passed - call message method */
		if (args.length == 0) {
			message();
		} else {
			/** Initializing all variables */
			boolean isMonthly = false;
			boolean isDaily = false;
			String resourceFileLocation = Constants.EMPTY;
			String job = Constants.EMPTY;
			String sourceDirectory = Constants.EMPTY;
			
			/** Assigning command line information to corresponding variable*/
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
				case "--Monthly":
				case "--monthly":
					isMonthly = true;
					break;
				case "--Daily":
				case "--daily":
					isDaily = true;
					break;
				case "--ResourceFileLocation":
				case "-resourcefilelocation":
					resourceFileLocation = args[i + 1];
					break;
				case "--Job":
				case "--job":
					job = args[i + 1];
					break;
				case "--InputFileLocation":
				case "--inputfilelocation":
					sourceDirectory = args[i + 1];
					break;
				}
			}
			
			/** At a time either Daily or Monthly data will be processed - if configuration files location is provided. Otherwise message method will be called */
			if ((isMonthly || isDaily) && !(isMonthly && isDaily) && !resourceFileLocation.isEmpty()) {
				Processor p = new Processor();
				
				/** Configuring Log4j properties */
				PropertyConfigurator.configure( resourceFileLocation + "Log4j.properties");
				if (job.isEmpty()) {
					job = "All";
				}
				logger.info( (isMonthly ? "************************** Monthly Job **************************" : "************************** Daily Job **************************"));
			/**	System.out.println((isMonthly ? "Monthly" : "Daily") + " Job Details");
				System.out.println("Resource File : " + resourceFileLocation);
				System.out.println("Job List: " + job); */
				p.execute(isMonthly, resourceFileLocation, job, sourceDirectory);
			} else {
				message();
			}
		}
	}
	
	/** The message method prints information about the command line arguments that needs to be passed while running the program.*/ 
	private static void message() {
		System.out.println("************ ETL Process git **************");
		System.out.println("Arguments needs to be valid!");
		System.out.println("Monthly Jobs Needs below arguments\n" + "	--Monthly 'Runs Monthly taks'\n"
				+ "	--ResourceFileLocation 'Specify Location of Property_Files'\n " + "	--Job 'Specify Job Name'\n" + "	--InputFileLocation 'Specify the location of input files'\n");
		System.out.println("Example: java -jar Alpha_2Mysql.jar --Monthly --ResourceFileLocation '/home/vmuser1192/Project/Alpha/resources/' --Job AWS  --InputFileLocation '/home/vmuser1192/Project/RDD/'\n");
		System.out.println("Daily Jobs Needs below arguments\n" + "	--Daily 'Runs Daily job' \n"
				+ "	--ResourceFileLocation 'Specify Location of Property Files'\n" + "	--Job 'Specify Job Name' \n" + "	--InputFileLocation 'Specify the location of input files'\n");
		
		System.out.println("Example: java -jar Alpha_2Mysql.jar --Daily --ResourceFileLocation '/home/vmuser1192/Project/Alpha/resources/' --Job VIP --InputFileLocation '/home/vmuser1192/Project/Daily/' \n");
		System.out.println("************ ETL Process *************");
	}
	
	/** The execute method reads configuration files, basic properties and calls processTask method for further execution.*/
	
	private void execute(boolean isMonthly, String resourceFileLocation, String job, String sourceDirectory) throws Exception {
		Connection connection = null;
		try {
			Configurations configurations = new Configurations();
			
			/** Reading configurations from config.properties file */
			Properties config = configurations.getConfigurations(resourceFileLocation, Constants.CONFIG_FILE);
			
			/** Reading configurations from localDB.properties file */
			connection = getConnection(
					configurations.getConfigurations(resourceFileLocation, config.getProperty(Constants.DB_FILE)));
			
			/** The monthly based load is performed here.*/
			if (isMonthly) {
				String[] monthlyTask = null;
				
				/** Getting the information of what all are the monthly job(s) to be performed */
				if (!job.isEmpty() && !job.equals("All")) {
					monthlyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.MONTHLY_TASK)) {
					monthlyTask = config.get(Constants.MONTHLY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				
				/** If at least one monthly job is available then below code will be executed. */
				if (monthlyTask != null && monthlyTask.length > 0) {
					
					/** Calling processTask method in loop for each type of monthly task. */
					for (String mT : monthlyTask) {
						int returnCode = -1;
						try {
							sourceDirectory = sourceDirectory.isEmpty() ? config.get("rddLocation").toString()
									: sourceDirectory;
							returnCode = processTask(resourceFileLocation, sourceDirectory, mT, config, true,
									connection);
						} catch (Exception e) {
							throw e;
						}
						if (returnCode != 0 && returnCode != 4) {
							throw new Exception("Invalid return Code [" + returnCode + "] for task [" + mT + "]");
						}
					}
				}
			}
			/** The daily based load is performed here.*/
			else {
				String[] dailyTask = null;
				if (!job.isEmpty() && !job.equals("All")) {
					dailyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.DAILY_TASK)) {
					dailyTask = config.get(Constants.DAILY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				if (dailyTask != null && dailyTask.length > 0) {
					
					/** Calling processTask method in loop for each type of daily task. */
					for (String dT : dailyTask) {
						int returnCode = -1;
						try {
							sourceDirectory = sourceDirectory.isEmpty() ? config.get("dailyLocation").toString()
									: sourceDirectory;
							returnCode = processTask(resourceFileLocation, sourceDirectory, dT, config, false,
									connection);
						} catch (Exception e) {
							throw e;
						}
						if (returnCode != 0 && returnCode != 4) {
							throw new Exception("Invalid return Code [" + returnCode + "] for task [" + dT + "]");
						}
						if (dT.contains("BBILNP")) 
						{
							callStoredProcedure(connection,resourceFileLocation);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error( "Exception: " + e );
		} finally {
			connection.close();
		}
		System.out.println("Completed ... ");
	}

	/** This processTask method reads all files in the given directory. If matching file is found in that directory corresponding to the task then another processTask method will be called for processing the matching file. */
	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection) throws Exception {
		File dir = new File(sourceDirectory);
		int i = 0,j = 0;
		boolean FileMatched = false;
		if(isMonthly || (!isMonthly&& !taskName.equals("VIP"))) 
		{
			for (File file : dir.listFiles()) {
				if (file.getName().contains(config.get(taskName).toString())) {
					FileMatched = true;
					j = fc.checkFileProcessedStatus(resourceFileLocation, file.getName());
					if (j==0)
					{
						file.delete();
						//System.out.println("File will be deleted from current directory");	
					}
					else if(j==1)
					{
						i = processTask(resourceFileLocation, sourceDirectory, taskName, config, isMonthly, connection, file.getName());
					}
				}
			}
		}
		//Daily job -> taskName == "VIP"
		else
		{
			String latestFile = fc.getLatestDailyFile(sourceDirectory,taskName);
			if(latestFile!=null)
			{
				FileMatched = true;
				i = processTask(resourceFileLocation, sourceDirectory, taskName, config, isMonthly, connection,
						latestFile);
				for (File file : dir.listFiles()) {
					if (file.getName().contains(config.get(taskName).toString())) {
						File afile = new File(sourceDirectory + "/" + file.getName());
						String newName = sourceDirectory + "OldFiles/" + file.getName();
						if (afile.renameTo(new File(newName))) {
							//
						}
					}
				}
			}else
			{
				FileMatched = false;
			}
		}
		
		if (!FileMatched)
		{
			logger.warn( "No matching file found for " + taskName + " job!" );
		}
		return i;
	}

	/** This processTask method calls the processfile method for reading the content of the file. If file has been read successfully then it will call moveFile method to move the processed file into sub directory (Processed)  */
	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection, String fileName) throws Exception {
		System.out.println("processTask : " + taskName);
		int returnCode = 0;
			
		/** Calling the processfile method for reading the content of the file and writing into database */
		int i = processfile(resourceFileLocation, sourceDirectory, taskName, config, fileName, connection, isMonthly);
		System.out.println("Processed : " + fileName + ":  RC [" + i + "]");
		
		/** If file has been processed successfully then the below code will move the processed file into sub directory. */
		if (i == 0) {
			fc.WritePropertiesFile(resourceFileLocation,fileName);
			logger.info( fileName + " file : processed successfully!");
			boolean isFileMoved = fc.moveFile(sourceDirectory, fileName);
			logger.info( fileName + " file : " + (isFileMoved ? "moved successfully!" : "move failed!"));
			if (!isFileMoved) {
				return 1;
			}
		
		/** If file is not processed successfully then the below code will print the error type. */
		} else if (i != 4) {
			//System.out.println(": File Error" + fileName);
			logger.error( fileName + " file : not processed! Error in file" );
			return 1;
		}
		return returnCode;
	}

	/** The processfile method reads the file and inserts the file content into corresponding table. */
	
	private int processfile(String resourceFileLocation, String sourceDirectory, String taskName, Properties config, String fileName,
			Connection connection, boolean isMonthly) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(sourceDirectory + fileName));
			String line = Constants.EMPTY;
			Configurations conf = new Configurations();
			/** Reading configurations from table.properties file and fetching columns corresponding to table*/
			Properties tableProperties = conf.getConfigurations(resourceFileLocation, Constants.TABLE_FILE);
			String columnName = tableProperties.getProperty(taskName + "_Columns");
			columnName = columnName.replace("|", ",");
			String create_stmt;
			if (taskName.equals("VIP")||taskName.equals("UBUP")||taskName.equals("DREP"))
			{
				create_stmt = "DROP TABLE if exists " + tableProperties.getProperty(taskName + "_Name");
				Statement stmt = connection.createStatement();
				stmt.executeUpdate(create_stmt);
			}
			 create_stmt = "CREATE TABLE if not exists " + tableProperties.getProperty(taskName + "_Name") + " ("
					+ columnName + ")";
			connection.prepareStatement(create_stmt).execute();
			System.out.println(create_stmt);
			String tablePropFileName = config.getProperty(taskName + "_Table");
			System.out.println(taskName + " :" + tablePropFileName);
			int lineNumber = 0;
			int numofRecords = 0;
			
			/** Reading each line in loop from the file and inserting the same into the table*/
			while ((line = br.readLine()) != null) {
				if ((isMonthly && lineNumber == 0)||(taskName.contains("UBUP") && lineNumber < 19)||(taskName.contains("DREP") && lineNumber < 14)) {
					lineNumber++;
					continue;
				}else if (taskName.contains("UBUP") && line.startsWith("Total"))
				{
					break;
				}
				line = line.replaceAll(new String("Â".getBytes("UTF-8"), "UTF-8"), "").replaceAll(new String(" ,"), ",");
				String query = "INSERT INTO " + tableProperties.getProperty(taskName + "_Name") + " values ("
						+ dumpData(resourceFileLocation, config.getProperty(taskName + "_Table"), line,
								tableProperties.getProperty(taskName + "_Delimiter"),
								tableProperties.getProperty(taskName + "_Columns"))
						+ ")";
				System.out.println("INSERT query : " + query);
				connection.prepareStatement(query).execute();
				System.out.println("INSERT Completed");
				numofRecords++;
			}
			logger.info( fileName + " file : " + numofRecords + " records inserted successfully!" );
		/** If not able to read and insert the file content into table then below code will be executed.*/
		} catch (java.io.FileNotFoundException e) {
			System.out.println("File skipped (Not Found) : " + fileName);
			logger.error( fileName + " file not processed! File not found!" );
			return 4;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	/** The dumpData method will return records (or columns) corresponding to the each line in the file. */
	private String dumpData(String fileLocation, String columnFile, String data, String delimiter, String columnDef)
			throws Exception {
				return new DataMapper().getUsageData(fileLocation, columnFile, data, delimiter, columnDef);
	}
	
	/** The getConnection method will connect to the database. */
	public Connection getConnection(Properties properties) throws Exception {
		Class.forName((String) properties.get("driver"));
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:mysql://" + properties.get(Constants.IP) + ":"
				+ properties.get(Constants.PORT) + "/" + properties.get(Constants.DB) + "?user="
				+ properties.get(Constants.USER) + "&password=" + properties.get(Constants.PASSWORD));
		//System.out.println("Connected Sucessfully to " + properties.get(Constants.IP));
		return connection;
	}
	
	private void callStoredProcedure(Connection connection, String resourceFileLocation) throws IOException
	{
		logger.info( "Calling stored procedure..." );
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(resourceFileLocation + "StoredProcedure.sql"));
			String line,ProcedureQuery="";
			while ((line = br.readLine()) != null) {
				ProcedureQuery = ProcedureQuery + line.trim() + " ";
			}
			Statement stmt =connection.createStatement(); 
			stmt.execute("DROP PROCEDURE IF EXISTS Get;");
			stmt.execute(ProcedureQuery);
			stmt.execute("{call Get}");
			logger.info( "Stored Procedure created successfully!" );
		}
		catch (Exception e) {
			logger.error( "Error while creating Stored Procedure!\n Exception: " + e );
			e.printStackTrace();
		} 
		finally
		{
			if (br != null)
				br.close();
		}
	}
	
	
	
}
