package etl.core;
import etl.config.Constants;
public class DataMapper {
	
	public String getUsageData(String fileLocation, String columnFileName, String data, String delimiter, String columnDef)
			throws Exception {
		String[] columnData = data.split(Constants.delimiters.get(delimiter.trim()));
		String[] columnDefArray = columnDef.split("\\|");
		String values = Constants.EMPTY;
		for (int i = 0; i < columnDefArray.length; i++) {
			String[] info = columnDefArray[i].trim().split(" ");
			if (columnData.length > i && (columnData[i] != null)) {
				if (info[1].contains("date") && !columnData[i].equals(Constants.EMPTY)) {
					if (columnData[i].contains("/")) {
						values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("date1")
								+ ")";
					} else {
						values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("date2")
								+ ")";
					}
				} else if (info[1].contains("time")) {
					values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("time") + ")";
				} else if (!info[1].contains("int") && !info[1].contains("double")) {
					values = values + "'" + columnData[i].replace("'", "''") + "'";
				} else {
					if (columnData[i].equals(Constants.EMPTY)) {
						values = values + "0";
					} else {
						values = values + columnData[i];
					}
				}
			} else {
				if (info[1].contains("int") || info[1].contains("double"))
					values = values + "0";
				else
					values = values + "''";
			}
			if (i != columnDefArray.length - 1) {
				values = values + Constants.DELIMITER_COMMA;
			}
		}
		return values;
	}
}
