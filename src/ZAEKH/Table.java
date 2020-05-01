package ZAEKH;

import java.awt.Polygon;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Table implements Serializable {
	ArrayList<String> arrPageLoc;
	ArrayList<Object> MinMaxKeys;
	String name;
	String primarykey;
	boolean newPage = false;

	public Table(String name, String primarykey) {
		arrPageLoc = new ArrayList<>();
		MinMaxKeys = new ArrayList<>();
		this.name = name;
		this.primarykey = primarykey;
	}


	public void serializeTable(Table table) throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("Z:/Desktop/ZAEKH/data/" + table.name + ".ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public void serializePage(Page page) throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream(
				"Z:/Desktop/ZAEKH/data/" + page.tableName + "_" + page.id + ".ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public boolean compare(Object x, Object y) {
		boolean f = false;

		if (x instanceof Integer) {
			if ((Integer) x >= (Integer) y)
				f = true;
			else
				f = false;
		}

		if (x instanceof Polygon) {

			int z = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
			int w = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
			if (z >= w) {
				f = true;
			} else
				f = false;

		}
		if (x instanceof Double) {
			if ((Double) x >= (Double) y)
				f = true;
			else
				f = false;
		}
		if (x instanceof String) {
			if (((String) x).toLowerCase().compareTo(((String) y).toLowerCase()) < 0)
				f = false;
			else
				f = true;
		}
		if (x instanceof Boolean) {
			if ((Boolean) y == false && (Boolean) x == true)
				f = true;
			else
				f = false;
		}
		if(x instanceof Date) {
			int dayX = ((Date) x).getDate();
			int monthX = ((Date) x).getMonth();
			int yearX = ((Date) x).getYear();
			
			int dayY = ((Date) y).getDate();
			int monthY = ((Date) y).getMonth();
			int yearY = ((Date) y).getYear();
			
			if(yearX > yearY)
				return true;
			else if(yearX < yearY)
				return false;
			else if(monthX > monthY)
				return true;
			else if(monthX < monthY)
				return false;
			else if(dayX >= dayY)
				return true;
			else if(dayX < dayY)
				return false;
		}
		
		return f;
	}


	public Page deserializePage(String pageLoc) {
		Page p = null;
		try {
			FileInputStream fileIn = new FileInputStream(pageLoc);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found");
			c.printStackTrace();

		}
		return p;
	}

	
}
