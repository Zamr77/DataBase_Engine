package ZAEKH;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page extends Vector implements Serializable {
	//public static int noOfobjects = 0;
	public int id;
	String tableName;
	
	
	
	public Page(String tableName) throws FileNotFoundException, DBAppException {
		DBApp dbApp = new DBApp();
		Table table = dbApp.deserializeTable(tableName);
		int max=0;
		for(int i=0;i<table.arrPageLoc.size();i++) {
			Page page = dbApp.deserializePage(table.arrPageLoc.get(i));
			if(max<page.id)
				max = page.id;
			dbApp.serializePage(page);
		}
		id = max+1;
		this.tableName = tableName;
	}

	public Properties loadProperty() {
		Properties prop = new Properties();

		try (InputStream input = new FileInputStream("config/DBApp.properties")) {

			// load a properties file
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return prop;
	}

	public boolean checker() {
		Properties properties = loadProperty();

		if (this.size() < Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"))) {

			return true;
		} else {
			return false;
		}
	}

	
}
