package ZAEKH;

import java.awt.Point;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

import javax.swing.RootPaneContainer;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class DBApp implements java.io.Serializable {
	ArrayList<String> tableNames;
	Hashtable<String, Hashtable<String, Object>> tableName_Column_Root;
	Hashtable<String, Hashtable<String, String>> metadataHash;
	static boolean insert = false;
	int oldLocation;
	Hashtable temp = null;
	int count = 0;
	static String polygonCompare = "";
	static boolean polygonSelect = false;
	static boolean updatePolygon = false;

	public DBApp() {
		tableNames = new ArrayList<>();
		tableName_Column_Root = new Hashtable<>();
		metadataHash = new Hashtable<>();
		oldLocation = 0;
	}

	///// for the first time only
	public void init() throws IOException, DBAppException {
		File folder = new File("data/");
		folder.mkdirs();
		File file = new File(folder, "metadata.csv");
		if (!file.exists()) {
			FileWriter metadata = new FileWriter("data/metadata.csv");
			metadata.append("Table Name,Column Name,Column Type,ClusteringKey,Indexed");
			metadata.close();
		}

		File file1 = new File(folder, "TableNames.csv");
		if (!file1.exists()) {
			FileWriter metadata = new FileWriter("data/TableNames.csv");
			metadata.close();
		}
		serializeTCRFile(tableName_Column_Root);
		serializeMetaFile(metadataHash);

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		tableNames = readTableNames("data/TableNames.csv");
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");

		if (metadataHash.get(strTableName) != null)
			throw new DBAppException("Table has been already created!");
		if (htblColNameType.get(strClusteringKeyColumn) == null)
			throw new DBAppException("No columns match the entered clustered key!");

		Enumeration enumeration = htblColNameType.keys();
		String string = "";
		while (enumeration.hasMoreElements()) {
			string = (String) enumeration.nextElement();
			if (!htblColNameType.get(string).equals("java.lang.Integer")
					&& !htblColNameType.get(string).equals("java.lang.String")
					&& !htblColNameType.get(string).equals("java.lang.Double")
					&& !htblColNameType.get(string).equals("java.lang.Boolean")
					&& !htblColNameType.get(string).equals("java.util.Date")
					&& !htblColNameType.get(string).equals("java.awt.Polygon"))
				throw new DBAppException("You have entered a datatype other than the six allowed ones!");
		}

		Table table = new Table(strTableName, strClusteringKeyColumn);
		htblColNameType.put("TouchDate", "java.util.Date");
		writeCSV("data/metadata.csv", htblColNameType, strClusteringKeyColumn, strTableName);
		tableNames.add(strTableName);
		serializeTable(table);
		tableName_Column_Root.put(strTableName, new Hashtable<>());
		serializeTCRFile(tableName_Column_Root);
		writeText("data/TableNames.csv", strTableName);

		table = null;

	}

	public void print(String x) {
		System.out.println(x);
	}

	public String readFile(String x) {
		File file = new File(x);

		String result = "";

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			print("File Not Found ");
		}

		String st;
		try {
			while ((st = br.readLine()) != null)
				result += st + "\n";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			print("File is Empty ");

		}

		return result;
	}

	public ArrayList<String> readTableNames(String x) {
		ArrayList result = new ArrayList<>();
		File file = new File(x);

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
		}
		String st;
		try {
			while ((st = br.readLine()) != null) {
				result.add(st);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block

		}

		return removeempty(result);
	}

	public ArrayList readFilePutinArraylist(String x, ArrayList<String> tableNames) {
		ArrayList<List<String>> result = new ArrayList<>();
		File file = new File(x);

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			print("File Not Found ");
		}
		String st;
		String[] x1;
		try {
			while ((st = br.readLine()) != null) {
				x1 = st.split(",", 5);
				result.add(Arrays.asList(x1));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			print("File is Empty ");

		}

		return result;
	}

	public void writeText(String file, String data) {
		File f = new File(file);

		String oldData = readFile(file);

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		try {
			writer.write(oldData + data);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		// checking for an existence index on same table and same column
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");
		tableNames = readTableNames("data/TableNames.csv");
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		if (metadataHash.get(strTableName) == null)
			throw new DBAppException("There is no such table in database");

		if (metadataHash.get(strTableName).get("index" + strColName) != null)
			throw new DBAppException("There is already an index on this column");

		if (metadataHash.get(strTableName).get(strColName) == null)
			throw new DBAppException("There is no such column in this table");

		if (metadataHash.get(strTableName).get(strColName).toLowerCase().equals("java.awt.polygon"))
			throw new DBAppException("You can't create B+ Tree for polygon");

		Table table = deserializeTable(strTableName);

		ArrayList<Hashtable<String, Object>> array = new ArrayList<>();
		for (int i = 0; i < table.arrPageLoc.size(); i++) {
			Page page = deserializePage(table.arrPageLoc.get(i));
			for (int j = 0; j < page.size(); j++) {
				Hashtable<String, Object> key = new Hashtable();
				key.put("key", ((Hashtable) (page.get(j))).get(strColName));
				key.put("location", table.arrPageLoc.get(i));
				array.add(key);
			}
			serializePage(page);
		}

		// create new index
		BPTreeIndex bpTreeIndex = new BPTreeIndex(strTableName);
		Node root = new Node(strTableName, strColName);
		bpTreeIndex.root = root;

		for (int i = 0; i < array.size(); i++) {
			bpTreeIndex.insert(bpTreeIndex.root, array.get(i).get("key"), array.get(i), strTableName, strColName);
		}

		tableName_Column_Root.get(strTableName).put(strColName, bpTreeIndex.root);
		tableName_Column_Root.get(strTableName).put(strColName + "id", bpTreeIndex.root.id);

		serializeTCRFile(tableName_Column_Root);

		updateCSV("data/metadata.csv", strTableName, strColName);

	}

	public void writeCSV(String fileToUpdate, Hashtable<String, String> htblColNameType, String strClusteringKeyColumn,
			String strTableName) throws IOException, DBAppException {
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		File inputFile = new File(fileToUpdate);

		// Read existing file
		CSVReader reader = new CSVReader(new FileReader(inputFile), ',');
		List<String[]> csvBody = reader.readAll();

		boolean cluster = false;
		int i = 0;
		Hashtable<String, String> temp = new Hashtable<>();

		Enumeration<String> keyss = htblColNameType.keys();
		while (keyss.hasMoreElements()) {
			cluster = false;
			String columnName = keyss.nextElement();
			if (columnName.equals(strClusteringKeyColumn)) {
				cluster = true;
				temp.put("cluster", columnName);
			}
			String[] x = new String[5];
			x[0] = strTableName;
			x[1] = columnName;
			x[2] = htblColNameType.get(columnName);
			x[3] = "" + cluster;
			x[4] = "false";
			csvBody.add(x);

			temp.put("column" + i, columnName);
			temp.put(columnName, htblColNameType.get(columnName));
			i++;
		}
		temp.put("columnSize", i + "");
		temp.put("indexSize", 0 + "");
		metadataHash.put(strTableName, temp);
		serializeMetaFile(metadataHash);

		reader.close();

		// Write to CSV file which is open
		CSVWriter writer = new CSVWriter(new FileWriter(inputFile), ',');
		writer.writeAll(csvBody);
		writer.flush();
		writer.close();

	}

	public void updateCSV(String fileToUpdate, String tableName, String columnName) throws IOException, DBAppException {
		File inputFile = new File(fileToUpdate);
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		// Read existing file
		CSVReader reader = new CSVReader(new FileReader(inputFile), ',');
		List<String[]> csvBody = reader.readAll();
		// get CSV row column and replace with by using row and column
		for (int i = 0; i < csvBody.size(); i++) {
			String[] strArray = csvBody.get(i);
			if (strArray[0].equals(tableName) && strArray[1].equals(columnName)) {
				csvBody.get(i)[4] = "TRUE";
			}

		}
		reader.close();

		// Write to CSV file which is open
		CSVWriter writer = new CSVWriter(new FileWriter(inputFile), ',');
		writer.writeAll(csvBody);
		writer.flush();
		writer.close();

		Hashtable<String, String> temp = metadataHash.get(tableName);
		int index = Integer.parseInt(temp.get("indexSize"));
		temp.put("index" + columnName, "true");
		temp.put("index" + index, columnName);
		index++;
		temp.put("indexSize", index + "");
		serializeMetaFile(metadataHash);
	}

	private ArrayList<ArrayList<Object>> sort(ArrayList<ArrayList<Object>> array) {
		Object value;
		for (int i = 1; i < array.size(); i++) {
			ArrayList<Object> x = array.get(i);
			value = (Object) array.get(i).get(0);
			int j = i - 1;
			while (j >= 0 && compare(array.get(j).get(0), value)) {
				// nums[j + 1] = nums[j];
				array.remove(j + 1);
				array.add(j + 1, array.get(j));
				// array.remove(j);
				j = j - 1;
			}
			// nums[j + 1] = value;
			array.remove(j + 1);
			array.add(j + 1, x);
		}
		return array;
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		// checking for an existence index on same table and same column
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");
		tableNames = readTableNames("data/TableNames.csv");
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		if (metadataHash.get(strTableName) == null)
			throw new DBAppException("There is no such table in database");

		if (metadataHash.get(strTableName).get("index" + strColName) != null)
			throw new DBAppException("There is already an index on this column");

		if (metadataHash.get(strTableName).get(strColName) == null)
			throw new DBAppException("There is no such column in this table");

		if (!metadataHash.get(strTableName).get(strColName).toLowerCase().equals("java.awt.polygon"))
			throw new DBAppException("You can't create R Tree for non-polygon key");

		Table table = deserializeTable(strTableName);

		ArrayList<Hashtable<String, Object>> array = new ArrayList<>();
		for (int i = 0; i < table.arrPageLoc.size(); i++) {
			Page page = deserializePage(table.arrPageLoc.get(i));
			for (int j = 0; j < page.size(); j++) {
				Hashtable<String, Object> key = new Hashtable();
				key.put("key", ((Hashtable) (page.get(j))).get(strColName));
				key.put("location", table.arrPageLoc.get(i));
				array.add(key);
			}
			serializePage(page);
		}

		// create new index
		RTreeIndex rTreeIndex = new RTreeIndex(strTableName);
		Node root = new Node(strTableName, strColName);
		rTreeIndex.root = root;

		polygonCompare = "insert";
		
		for (int i = 0; i < array.size(); i++) {
			rTreeIndex.insert(rTreeIndex.root, array.get(i).get("key"), array.get(i), strTableName, strColName);
		}

		tableName_Column_Root.get(strTableName).put(strColName, rTreeIndex.root);
		tableName_Column_Root.get(strTableName).put(strColName + "id", rTreeIndex.root.id);

		serializeTCRFile(tableName_Column_Root);

		updateCSV("data/metadata.csv", strTableName, strColName);
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

			switch (polygonCompare) {
			case "insert":
				int z = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
				int w = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
				if (z >= w) {
					f = true;
				} else
					f = false;
				break;

			case "select":
				// polygonSelect = true? ----> =/!=
				if (polygonSelect) {
					return compareCoordinates((Polygon) x, (Polygon) y);
				} else {
					int z1 = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
					int w1 = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
					if (z1 >= w1) {
						f = true;
					} else
						f = false;
				}
				break;
			default:
				if (updatePolygon) {
					int z1 = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
					int w1 = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
					if (z1 >= w1) {
						f = true;
					} else
						f = false;
				} else
					return compareCoordinates((Polygon) x, (Polygon) y);
			}

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
		if (x instanceof Date) {
			int dayX = ((Date) x).getDate();
			int monthX = ((Date) x).getMonth();
			int yearX = ((Date) x).getYear();

			int dayY = ((Date) y).getDate();
			int monthY = ((Date) y).getMonth();
			int yearY = ((Date) y).getYear();

			if (yearX > yearY)
				return true;
			else if (yearX < yearY)
				return false;
			else if (monthX > monthY)
				return true;
			else if (monthX < monthY)
				return false;
			else if (dayX >= dayY)
				return true;
			else if (dayX < dayY)
				return false;
		}

		return f;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		insertIntoTableHelper(strTableName, htblColNameValue, true, -1);
	}

	public void insertIntoTableHelper(String strTableName, Hashtable<String, Object> htblColNameValue, Boolean newTuple,
			int old) throws DBAppException, IOException {
		tableNames = readTableNames("data/TableNames.csv");
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");
		polygonCompare = "insert";
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		// checking that strTableName is existing table
		if (metadataHash.get(strTableName) == null)
			throw new DBAppException("There is no such table in your database.");

		Table table = deserializeTable(strTableName);

		if (newTuple) {

			boolean indecesFlag = false;
			Node root = null;

			Date dNow = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			int day = Integer.parseInt(ft.format(dNow).substring(8, 10));
			int month = Integer.parseInt(ft.format(dNow).substring(5, 7));
			int year = Integer.parseInt(ft.format(dNow).substring(0, 4));
			int hour = Integer.parseInt(ft.format(dNow).substring(11, 13));
			int minute = Integer.parseInt(ft.format(dNow).substring(14, 16));
			int second = Integer.parseInt(ft.format(dNow).substring(17, 19));

			Date date = new Date(year, month, day, hour, minute, second);
			htblColNameValue.put("TouchDate", date);

			String columnName = "";
			for (int i = 0; i < Integer.parseInt(metadataHash.get(strTableName).get("columnSize") + ""); i++) {
				columnName = metadataHash.get(strTableName).get("column" + i);
				if (htblColNameValue.get(columnName) == null)
					throw new DBAppException("Please make sure that you have inserted all columns.");
				else if (!metadataHash.get(strTableName).get(columnName).toLowerCase()
						.equals(htblColNameValue.get(columnName).getClass().getName().toLowerCase()))
					throw new DBAppException("Please enter the correct dataType.");

			}

			Enumeration enu1 = htblColNameValue.keys();
			String newString = "";

			while (enu1.hasMoreElements()) {
				newString = (String) enu1.nextElement();
				if (metadataHash.get(strTableName).get(newString) == null)
					throw new DBAppException("You have entered extra columns");
			}

		}
		int indexOfPage = 0;
		updatePolygon = true;
		if (metadataHash.get(strTableName).get("index" + table.primarykey) != null)
			indexOfPage = findPageByIndex(htblColNameValue.get(table.primarykey),
					(Node) tableName_Column_Root.get(strTableName).get(table.primarykey), strTableName, table);
		else
			indexOfPage = searchPage(table, table.MinMaxKeys, htblColNameValue.get(table.primarykey), table.arrPageLoc);

		updatePolygon = false;

		if (indexOfPage == -1) {
			Page page = new Page(strTableName);
			page.add(htblColNameValue);
			table.MinMaxKeys.add(htblColNameValue.get(table.primarykey));
			table.MinMaxKeys.add(htblColNameValue.get(table.primarykey));
			table.arrPageLoc.add("data/" + strTableName + "_" + page.id + ".ser");
			serializePage(page);
			serializeTable(table);
			page = null;
			indexOfPage = table.arrPageLoc.size() - 1;

		} else {

			Page page = deserializePage(table.arrPageLoc.get(indexOfPage));
			int indexInPage = 0;
			if (page.size() != 0) {
				indexInPage = searchToInsertInPage(page, htblColNameValue.get(table.primarykey), table);
			}

			if (page.checker()) {
				if (indexInPage == page.size()) {
					page.add(htblColNameValue);
					if (compare(table.MinMaxKeys.get(indexOfPage * 2), htblColNameValue.get(table.primarykey))) {
						table.MinMaxKeys.set(indexOfPage * 2, htblColNameValue.get(table.primarykey));
					}
					if (compare(htblColNameValue.get(table.primarykey), table.MinMaxKeys.get(indexOfPage * 2 + 1))) {
						table.MinMaxKeys.set(indexOfPage * 2 + 1, htblColNameValue.get(table.primarykey));
					}

					serializePage(page);
					serializeTable(table);
					page = null;
				} else {
					page.add(indexInPage, htblColNameValue);
					if (compare(table.MinMaxKeys.get(indexOfPage * 2), htblColNameValue.get(table.primarykey))) {
						table.MinMaxKeys.set(indexOfPage * 2, htblColNameValue.get(table.primarykey));
					}
					if (compare(htblColNameValue.get(table.primarykey), table.MinMaxKeys.get(indexOfPage * 2 + 1))) {
						table.MinMaxKeys.set(indexOfPage * 2 + 1, htblColNameValue.get(table.primarykey));
					}

					serializePage(page);
					serializeTable(table);
					page = null;
				}
			} else {
				oldLocation = indexOfPage;
				temp = (Hashtable) page.get(page.size() - 1);
				page.add(indexInPage, htblColNameValue);
				page.remove(page.size() - 1);
				table.MinMaxKeys.set(indexOfPage * 2 + 1,
						((Hashtable) (page.get(page.size() - 1))).get(table.primarykey));

				if (compare(table.MinMaxKeys.get(indexOfPage * 2), htblColNameValue.get(table.primarykey))) {
					table.MinMaxKeys.set(indexOfPage * 2, htblColNameValue.get(table.primarykey));
				}
				if (compare(htblColNameValue.get(table.primarykey), table.MinMaxKeys.get(indexOfPage * 2 + 1))) {
					table.MinMaxKeys.set(indexOfPage * 2 + 1, htblColNameValue.get(table.primarykey));
				}

				serializePage(page);
				serializeTable(table);
				page = null;

				insertIntoTableHelper(strTableName, temp, false, oldLocation);

			}
		}

		if (newTuple) {
			String columnName = "";
			for (int i = 0; i < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); i++) {
				columnName = metadataHash.get(strTableName).get("index" + i);
				if (metadataHash.get(strTableName).get(columnName).toLowerCase().equals("java.awt.polygon")) {
					RTreeIndex rTreeIndex = new RTreeIndex(strTableName);
					rTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

					Hashtable<String, Object> ptr = new Hashtable<>();
					ptr.put("key", htblColNameValue.get(columnName));
					ptr.put("location", table.arrPageLoc.get(indexOfPage));
					rTreeIndex.root.id = (int) tableName_Column_Root.get(strTableName).get(columnName + "id");
					rTreeIndex.insert(rTreeIndex.root, htblColNameValue.get(columnName), ptr, strTableName, columnName);
					tableName_Column_Root.get(strTableName).put(columnName, rTreeIndex.root);
					tableName_Column_Root.get(strTableName).put(columnName + "id", rTreeIndex.root.id);

				} else {
					BPTreeIndex bpTreeIndex = new BPTreeIndex(strTableName);
					bpTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

					Hashtable<String, Object> ptr = new Hashtable<>();
					ptr.put("key", htblColNameValue.get(columnName));
					ptr.put("location", table.arrPageLoc.get(indexOfPage));
					bpTreeIndex.root.id = (int) tableName_Column_Root.get(strTableName).get(columnName + "id");
					bpTreeIndex.insert(bpTreeIndex.root, htblColNameValue.get(columnName), ptr, strTableName,
							columnName);
					tableName_Column_Root.get(strTableName).put(columnName, bpTreeIndex.root);
					tableName_Column_Root.get(strTableName).put(columnName + "id", bpTreeIndex.root.id);
				}
			}
			polygonCompare = "";

		}

		if (!newTuple) {
			// updating pageLoc of the shifted tuple
			BPTreeIndex bpTreeIndex = new BPTreeIndex(strTableName);

			String columnName = "";
			for (int i = 0; i < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); i++) {
				columnName = metadataHash.get(strTableName).get("index" + i);
				updatePolygon = true;
				bpTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);
				bpTreeIndex.updateTuplePtr(bpTreeIndex.root, table.arrPageLoc.get(old),
						table.arrPageLoc.get(indexOfPage), htblColNameValue.get(columnName), strTableName);
				updatePolygon = false;
				tableName_Column_Root.get(strTableName).put(columnName, bpTreeIndex.root);
			}
		}
		serializeTCRFile(tableName_Column_Root);

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException {
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");
		tableNames = readTableNames("data/TableNames.csv");
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");

		if (htblColNameValue.isEmpty())
			return;

		if (metadataHash.get(strTableName) == null)
			throw new DBAppException("There is no such table in your database.");

		Table table = deserializeTable(strTableName);

		Enumeration enu1 = htblColNameValue.keys();
		String newString = "";
		while (enu1.hasMoreElements()) {
			newString = (String) enu1.nextElement();
			if (metadataHash.get(strTableName).get(newString) == null)
				throw new DBAppException("You have entered extra columns");
			else if (!metadataHash.get(strTableName).get(newString).toLowerCase()
					.equals(htblColNameValue.get(newString).getClass().getName().toLowerCase()))
				throw new DBAppException("Please Enter the Correct DataType");

		}

		Enumeration enu2 = htblColNameValue.keys();
		Enumeration enu3 = htblColNameValue.keys();
		BPTreeIndex index = new BPTreeIndex(strTableName);

		boolean f = false;
		boolean cluster = false;
		Object key = null;
		Object clusterkey = htblColNameValue.get(table.primarykey);

		boolean clusterAndIndex = false;
		boolean clusterAndNonIndex = false;
		boolean nonClusteredIndex = false;
		int x = 0;

		if (htblColNameValue.get(table.primarykey) != null) {

			if (tableName_Column_Root.get(strTableName).get(table.primarykey) != null) {
				clusterAndIndex = true;
				index.root = (Node) tableName_Column_Root.get(strTableName).get(table.primarykey);
				key = htblColNameValue.get(table.primarykey);
			} else
				clusterAndNonIndex = true;
		}

		if (!clusterAndIndex) {
			Enumeration enumeration = htblColNameValue.keys();
			String s = "";

			while (enumeration.hasMoreElements()) {
				s = (String) enumeration.nextElement();
				if (!table.primarykey.equals(s) && metadataHash.get(strTableName).get("index" + s) != null) {
					nonClusteredIndex = true;
					index.root = (Node) tableName_Column_Root.get(strTableName).get(s);
					key = htblColNameValue.get(s);
					break;
				}
			}
		}

		// delete using clustered index
		if (clusterAndIndex) {
			updatePolygon = true;
			ArrayList<String> locations = index.searchForLocation(strTableName, index.root, key);
			updatePolygon = false;
			ArrayList<String> disLocations = distinctString(locations);
			ArrayList<Integer> positions = new ArrayList<>();
			boolean delete = true;

			Page page;
			for (int i = 0; i < disLocations.size(); i++) {
				page = deserializePage(disLocations.get(i));
				positions = searchBinaryToDeleteFromPage(page, key, table, table.primarykey);
				positions = sortPositions(positions);
				for (int j = positions.size() - 1; j >= 0; j--) {
					delete = true;
					enu3 = htblColNameValue.keys();
					String string2;
					while (enu3.hasMoreElements()) {
						string2 = (String) enu3.nextElement();
						if (!equals(htblColNameValue.get(string2),
								(((Hashtable) (page.get(positions.get(j)))).get(string2)))) {
							delete = false;
							break;
						}
					}
					if (delete) {
						String columnName;
						for (int m = 0; m < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); m++) {
							columnName = metadataHash.get(strTableName).get("index" + m);

							index.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

							updatePolygon = true;

							if (index.countOfTuples(index.root,
									((Hashtable) (page.get(positions.get(j)))).get(columnName), strTableName) == 1)
								index.delete(index.root, ((Hashtable) (page.get(positions.get(j)))).get(columnName));

							else
								index.deleteOneTuplePtr(index.root,
										((Hashtable) (page.get(positions.get(j)))).get(columnName), disLocations.get(i),
										strTableName);

							tableName_Column_Root.get(strTableName).put(columnName, index.root);

							updatePolygon = false;

						}

						int ind = positions.get(j);
						page.remove(ind);
						int q = table.arrPageLoc.indexOf(disLocations.get(i));
						if (page.size() == 0) {
							table.arrPageLoc.remove("data/" + page.tableName + "_" + page.id + ".ser");
							File file = new File("data/" + page.tableName + "_" + page.id + ".ser");
							file.delete();
							table.MinMaxKeys.remove(q * 2);
							table.MinMaxKeys.remove(q * 2);
						} else {
							if (ind == 0)
								table.MinMaxKeys.set(q * 2, ((Hashtable) (page.get(0))).get(table.primarykey));
							if (ind == page.size())
								table.MinMaxKeys.set(q * 2 + 1,
										((Hashtable) (page.get(page.size() - 1))).get(table.primarykey));
						}

					}

				}
				serializePage(page);
			}
		}

		// delete using non clustered index
		else if (nonClusteredIndex)

		{
			updatePolygon = true;
			ArrayList<String> locations = index.searchForLocation(strTableName, index.root, key);
			updatePolygon = false;
			ArrayList<String> disLocations = distinctString(locations);
			boolean delete = true;

			Page page;
			for (int i = 0; i < disLocations.size(); i++) {
				page = deserializePage(disLocations.get(i));
				for (int j = 0; j < page.size(); j++) {
					delete = true;
					enu3 = htblColNameValue.keys();
					String string2;
					while (enu3.hasMoreElements()) {
						string2 = (String) enu3.nextElement();
						updatePolygon = false;
						if (!equals(htblColNameValue.get(string2), (((Hashtable) (page.get(j))).get(string2)))) {
							delete = false;
							break;
						}
					}

					if (delete) {
						String columnName;
						for (int m = 0; m < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); m++) {
							columnName = metadataHash.get(strTableName).get("index" + m);

							index.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

							updatePolygon = true;

							if (index.countOfTuples(index.root, ((Hashtable) (page.get(j))).get(columnName),
									strTableName) == 1)
								index.delete(index.root, ((Hashtable) (page.get(j))).get(columnName));
							else
								index.deleteOneTuplePtr(index.root, ((Hashtable) (page.get(j))).get(columnName),
										disLocations.get(i), strTableName);

							tableName_Column_Root.get(strTableName).put(columnName, index.root);

							updatePolygon = false;

						}
						int ind = j;
						page.remove(ind);
						int q = table.arrPageLoc.indexOf(disLocations.get(i));
						if (page.size() == 0) {
							table.arrPageLoc.remove("data/" + page.tableName + "_" + page.id + ".ser");
							File file = new File("data/" + page.tableName + "_" + page.id + ".ser");
							file.delete();
							table.MinMaxKeys.remove(q * 2);
							table.MinMaxKeys.remove(q * 2);
						} else {
							if (ind == 0)
								table.MinMaxKeys.set(q * 2, ((Hashtable) (page.get(0))).get(table.primarykey));
							if (ind == page.size())
								table.MinMaxKeys.set(q * 2 + 1,
										((Hashtable) (page.get(page.size() - 1))).get(table.primarykey));
						}
						j--;
					}
				}
				serializePage(page);

			}
		} // delete using binary
		else if (clusterAndNonIndex) {
			updatePolygon = true;
			ArrayList<Integer> indecesOfPages = searchPages(clusterkey, table.MinMaxKeys, table);
			updatePolygon = false;
			indecesOfPages = distinctInteger(indecesOfPages);
			ArrayList<Integer> positions = new ArrayList<>();
			boolean delete = true;
			Page page;
			for (int i = 0; i < indecesOfPages.size(); i++) {
				page = deserializePage(table.arrPageLoc.get(indecesOfPages.get(i)));
				positions = searchBinaryToDeleteFromPage(page, clusterkey, table, table.primarykey);
				positions = sortPositions(positions);
				for (int j = positions.size() - 1; j >= 0; j--) {
					delete = true;
					enu3 = htblColNameValue.keys();
					String string2;
					while (enu3.hasMoreElements()) {
						string2 = (String) enu3.nextElement();
						updatePolygon = false;
						if (!equals(htblColNameValue.get(string2),
								(((Hashtable) (page.get(positions.get(j)))).get(string2)))) {
							delete = false;
							break;
						}
					}

					if (delete) {
						String columnName;
						for (int m = 0; m < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); m++) {
							columnName = metadataHash.get(strTableName).get("index" + m);

							index.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

							updatePolygon = true;

							if (index.countOfTuples(index.root,
									((Hashtable) (page.get(positions.get(j)))).get(columnName), strTableName) == 1)
								index.delete(index.root, ((Hashtable) (page.get(j))).get(columnName));
							else
								index.deleteOneTuplePtr(index.root,
										((Hashtable) (page.get(positions.get(j)))).get(columnName),
										table.arrPageLoc.get(i), strTableName);

							tableName_Column_Root.get(strTableName).put(columnName, index.root);

							updatePolygon = false;
						}

						int ind = positions.get(j);
						page.remove(ind);
						int q = table.arrPageLoc.indexOf("data/" + page.tableName + "_" + page.id + ".ser");
						if (page.size() == 0) {
							table.arrPageLoc.remove("data/" + page.tableName + "_" + page.id + ".ser");
							File file = new File("data/" + page.tableName + "_" + page.id + ".ser");
							file.delete();
							table.MinMaxKeys.remove(q * 2);
							table.MinMaxKeys.remove(q * 2);
						} else {
							if (ind == 0)
								table.MinMaxKeys.set(q * 2, ((Hashtable) (page.get(0))).get(table.primarykey));
							if (ind == page.size())
								table.MinMaxKeys.set(q * 2 + 1,
										((Hashtable) (page.get(page.size() - 1))).get(table.primarykey));
						}
					}
				}
				serializePage(page);

			}
		} // linearly
		else {

			Page page;
			for (int i = 0; i < table.arrPageLoc.size(); i++) {
				page = deserializePage(table.arrPageLoc.get(i));
				for (int j = 0; j < page.size(); j++) {
					Enumeration enu = htblColNameValue.keys();
					boolean delete = true;
					String newString1;
					while (enu.hasMoreElements()) {
						newString1 = (String) enu.nextElement();
						updatePolygon = false;
						if (!equals(htblColNameValue.get(newString1), (((Hashtable) page.get(j)).get(newString1)))) {
							delete = false;
							break;
						}

					}
					if (delete) {
						String columnName;
						for (int m = 0; m < Integer.parseInt(metadataHash.get(strTableName).get("indexSize")); m++) {
							columnName = metadataHash.get(strTableName).get("index" + m);

							index.root = (Node) tableName_Column_Root.get(strTableName).get(columnName);

							updatePolygon = true;

							if (index.countOfTuples(index.root, ((Hashtable) page.get(j)).get(columnName),
									strTableName) == 1)
								index.delete(index.root, ((Hashtable) (page.get(j))).get(columnName));
							else
								index.deleteOneTuplePtr(index.root, ((Hashtable) page.get(j)).get(columnName),
										table.arrPageLoc.get(i), strTableName);

							tableName_Column_Root.get(strTableName).put(columnName, index.root);

							updatePolygon = false;

						}

						page.remove(page.get(j));
						int q = table.arrPageLoc.indexOf("data/" + page.tableName + "_" + page.id + ".ser");

						if (j == 0)
							table.MinMaxKeys.set(q * 2, ((Hashtable) (page.get(0))).get(table.primarykey));
						if (j == page.size())
							table.MinMaxKeys.set(q * 2 + 1,
									((Hashtable) (page.get(page.size() - 1))).get(table.primarykey));
						j--;
						if (page.size() == 0) {
							table.arrPageLoc.remove("data/" + page.tableName + "_" + page.id + ".ser");
							File file = new File("data/" + page.tableName + "_" + page.id + ".ser");
							file.delete();
							break;
						}
					}
					serializePage(page);
				}
			}
		}
		serializeTable(table);
		serializeTCRFile(tableName_Column_Root);
		table = null;

	}

	public ArrayList<String> removeempty(ArrayList<String> x) {
		for (int i = 0; i < x.size(); i++) {
			if (x.get(0).equals(""))
				x.remove(i);
		}
		return x;

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		if (htblColNameValue.isEmpty())
			return;

		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");
		tableNames = readTableNames("data/TableNames.csv");
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");
		updatePolygon = true;

		if (metadataHash.get(strTableName) == null)
			throw new DBAppException("There is no such table in your database.");

		Enumeration enu1 = htblColNameValue.keys();
		String newString1 = "";
		while (enu1.hasMoreElements()) {
			newString1 = (String) enu1.nextElement();
			if (metadataHash.get(strTableName).get(newString1) == null)
				throw new DBAppException("You have entered extra columns");
			else if (!metadataHash.get(strTableName).get(newString1).toLowerCase()
					.equals(htblColNameValue.get(newString1).getClass().getName().toLowerCase()))
				throw new DBAppException("Please Enter the Correct DataType");

		}

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		int day = Integer.parseInt(ft.format(dNow).substring(8, 10));
		int month = Integer.parseInt(ft.format(dNow).substring(5, 7));
		int year = Integer.parseInt(ft.format(dNow).substring(0, 4));
		int hour = Integer.parseInt(ft.format(dNow).substring(11, 13));
		int minute = Integer.parseInt(ft.format(dNow).substring(14, 16));
		int second = Integer.parseInt(ft.format(dNow).substring(17, 19));

		Date date = new Date(year, month, day, hour, minute, second);

		Table table = deserializeTable(strTableName);

		String keyType = "";
		String clusterKey = "";
		boolean clusterAndIndex = false;

		clusterKey = metadataHash.get(strTableName).get("cluster");
		keyType = metadataHash.get(strTableName).get(clusterKey);

		if (metadataHash.get(strTableName).get("index" + clusterKey) != null) {
			clusterAndIndex = true;
		}

		if (htblColNameValue.get(clusterKey) != null)
			throw new DBAppException("We can't update the cluster key");

		Object key = null;

		if (keyType.equals("java.lang.Integer")) {
			key = Integer.parseInt(strKey);
		} else if (keyType.equals("java.lang.Double")) {
			key = Double.parseDouble(strKey);
		} else if (keyType.equals("java.lang.Boolean")) {
			strKey = strKey.toLowerCase();
			if (strKey.equals("true"))
				key = true;
			else
				key = false;
		} else if (keyType.equals("java.util.Date")) {
			int day1 = Integer.parseInt(strKey.substring(8, 10));
			int month1 = Integer.parseInt(strKey.substring(5, 7));
			int year1 = Integer.parseInt(strKey.substring(0, 4));
			key = new Date(year1, month1, day1);
		} else { ///// parsing polygons
			Polygon polygon = new Polygon();
			StringTokenizer ss = new StringTokenizer(strKey, ")");

			String sr;
			String z = "";
			String q;
			String w;
			Point p;
			int c = 0;

			while (ss.hasMoreTokens()) {
				sr = ss.nextToken();
				z = "";
				sr += ")";
				if (sr.charAt(0) == ',')
					sr = sr.substring(1);

				for (int h = 0; h < sr.length(); h++) {
					if (sr.charAt(h) != '(' || sr.charAt(h) != ')') {
						z += sr.charAt(h);
					}
				}
				StringTokenizer r = new StringTokenizer(z, ",");

				q = r.nextToken();
				w = r.nextToken();
				int e = Integer.parseInt(q.substring(1));
				int a = Integer.parseInt(w.substring(0, w.length() - 1));
				polygon.addPoint(e, a);
			}
			key = polygon;
		}

		BPTreeIndex index = new BPTreeIndex(strTableName);
		BPTreeIndex index2 = new BPTreeIndex(strTableName);
		RTreeIndex rTreeIndex = new RTreeIndex(strTableName);

		if (tableName_Column_Root.get(strTableName).get(clusterKey) != null) {
			if (keyType.toLowerCase().equals("java.awt.polygon"))
				rTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(clusterKey);
			else
				index.root = (Node) tableName_Column_Root.get(strTableName).get(clusterKey);
		}
		// checks if cluster key has index
		if (clusterAndIndex) {
			ArrayList<String> locations;
			updatePolygon = true;
			if (keyType.toLowerCase().equals("java.awt.polygon"))
				locations = rTreeIndex.searchForLocation(strTableName, rTreeIndex.root, key);
			else
				locations = index.searchForLocation(strTableName, index.root, key);
			updatePolygon = false;

			ArrayList<String> disLocations = distinctString(locations);
			ArrayList<Integer> positions = new ArrayList<>();
			Page page = null;
			String newString;
			for (int i = 0; i < disLocations.size(); i++) {
				page = deserializePage(disLocations.get(i));
				positions = searchBinaryToDeleteFromPage(page, key, table, table.primarykey);
				positions = sortPositions(positions);
				for (int j = positions.size() - 1; j >= 0; j--) {
					if (!(key instanceof Polygon)) {
						enu1 = htblColNameValue.keys();
						while (enu1.hasMoreElements()) {
							newString = (String) enu1.nextElement();

							if (tableName_Column_Root.get(strTableName).get(newString) != null) {
								if (metadataHash.get(strTableName).get(newString).toLowerCase()
										.equals("java.awt.polygon")) {
									rTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

									updatePolygon = true;

									if (rTreeIndex.countOfTuples(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											strTableName) == 1)
										rTreeIndex.delete(rTreeIndex.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString));
									else
										rTreeIndex.deleteOneTuplePtr(rTreeIndex.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString),
												disLocations.get(i), strTableName);

									Hashtable<String, Object> ptr = new Hashtable<>();
									ptr.put("key", htblColNameValue.get(newString));
									ptr.put("location", disLocations.get(i));
									rTreeIndex.root.id = (int) tableName_Column_Root.get(strTableName)
											.get(newString + "id");
									rTreeIndex.insert(rTreeIndex.root, htblColNameValue.get(newString), ptr,
											strTableName, newString);
									tableName_Column_Root.get(strTableName).put(newString + "id", rTreeIndex.root.id);
									tableName_Column_Root.get(strTableName).put(newString, rTreeIndex.root);

									updatePolygon = false;

								} else {

									index2.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

									if (index2.countOfTuples(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											strTableName) == 1)
										index2.delete(index2.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString));
									else
										index2.deleteOneTuplePtr(index2.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString),
												disLocations.get(i), strTableName);

									Hashtable<String, Object> ptr = new Hashtable<>();
									ptr.put("key", htblColNameValue.get(newString));
									ptr.put("location", disLocations.get(i));
									index2.root.id = (int) tableName_Column_Root.get(strTableName)
											.get(newString + "id");
									index2.insert(index2.root, htblColNameValue.get(newString), ptr, strTableName,
											newString);
									tableName_Column_Root.get(strTableName).put(newString + "id", index2.root.id);
									tableName_Column_Root.get(strTableName).put(newString, index2.root);
								}
							}

							((Hashtable) page.get(positions.get(j))).replace(newString,
									htblColNameValue.get(newString));
							((Hashtable) page.get(positions.get(j))).replace("TouchDate", date);

						}
					} else if (compareCoordinates(
							(Polygon) ((Hashtable) page.get(positions.get(j))).get(table.primarykey), (Polygon) key)) {

						enu1 = htblColNameValue.keys();
						while (enu1.hasMoreElements()) {
							newString = (String) enu1.nextElement();

							if (tableName_Column_Root.get(strTableName).get(newString) != null) {
								if (metadataHash.get(strTableName).get(newString).toLowerCase()
										.equals("java.awt.polygon")) {
									rTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

									updatePolygon = true;

									if (rTreeIndex.countOfTuples(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											strTableName) == 1)
										rTreeIndex.delete(rTreeIndex.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString));
									else
										rTreeIndex.deleteOneTuplePtr(rTreeIndex.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString),
												disLocations.get(i), strTableName);

									Hashtable<String, Object> ptr = new Hashtable<>();
									ptr.put("key", htblColNameValue.get(newString));
									ptr.put("location", disLocations.get(i));
									rTreeIndex.root.id = (int) tableName_Column_Root.get(strTableName)
											.get(newString + "id");
									rTreeIndex.insert(rTreeIndex.root, htblColNameValue.get(newString), ptr,
											strTableName, newString);
									tableName_Column_Root.get(strTableName).put(newString + "id", rTreeIndex.root.id);
									tableName_Column_Root.get(strTableName).put(newString, rTreeIndex.root);

									updatePolygon = false;

								} else {

									index2.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

									if (index2.countOfTuples(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											strTableName) == 1)
										index2.delete(index2.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString));
									else
										index2.deleteOneTuplePtr(index2.root,
												((Hashtable) (page.get(positions.get(j)))).get(newString),
												disLocations.get(i), strTableName);

									Hashtable<String, Object> ptr = new Hashtable<>();
									ptr.put("key", htblColNameValue.get(newString));
									ptr.put("location", disLocations.get(i));
									index2.root.id = (int) tableName_Column_Root.get(strTableName)
											.get(newString + "id");
									index2.insert(index2.root, htblColNameValue.get(newString), ptr, strTableName,
											newString);
									tableName_Column_Root.get(strTableName).put(newString + "id", index2.root.id);
									tableName_Column_Root.get(strTableName).put(newString, index2.root);
								}
							}

							((Hashtable) page.get(positions.get(j))).replace(newString,
									htblColNameValue.get(newString));
							((Hashtable) page.get(positions.get(j))).replace("TouchDate", date);

						}
					}
				}
				serializePage(page);
			}

		} else

		{

			updatePolygon = true;
			ArrayList<Integer> locations = searchPages(key, table.MinMaxKeys, table);
			updatePolygon = false;
			locations = distinctInteger(locations);
			ArrayList<Integer> positions = new ArrayList<>();
			Enumeration enu3 = htblColNameValue.keys();
			Page page = null;
			String newString;

			for (int i = 0; i < locations.size(); i++) {
				page = deserializePage(table.arrPageLoc.get(locations.get(i)));
				positions = searchBinaryToDeleteFromPage(page, key, table, table.primarykey);
				positions = sortPositions(positions);

				for (int j = positions.size() - 1; j >= 0; j--) {

					enu3 = htblColNameValue.keys();
					while (enu3.hasMoreElements()) {
						newString = (String) enu3.nextElement();

						if (tableName_Column_Root.get(strTableName).get(newString) != null) {

							if (metadataHash.get(strTableName).get(newString).toLowerCase()
									.equals("java.awt.polygon")) {
								rTreeIndex.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

								updatePolygon = true;

								if (rTreeIndex.countOfTuples(rTreeIndex.root,
										((Hashtable) (page.get(positions.get(j)))).get(newString), strTableName) == 1)
									rTreeIndex.delete(rTreeIndex.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString));
								else
									rTreeIndex.deleteOneTuplePtr(rTreeIndex.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											table.arrPageLoc.get(locations.get(i)), strTableName);

								Hashtable<String, Object> ptr = new Hashtable<>();
								ptr.put("key", htblColNameValue.get(newString));
								ptr.put("location", table.arrPageLoc.get(locations.get(i)));
								rTreeIndex.root.id = (int) tableName_Column_Root.get(strTableName)
										.get(newString + "id");
								rTreeIndex.insert(rTreeIndex.root, htblColNameValue.get(newString), ptr, strTableName,
										newString);
								tableName_Column_Root.get(strTableName).put(newString + "id", rTreeIndex.root.id);
								tableName_Column_Root.get(strTableName).put(newString, rTreeIndex.root);

								updatePolygon = false;

							} else {

								index2.root = (Node) tableName_Column_Root.get(strTableName).get(newString);

								if (index2.countOfTuples(index2.root,
										((Hashtable) (page.get(positions.get(j)))).get(newString), strTableName) == 1)
									index2.delete(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString));
								else
									index2.deleteOneTuplePtr(index2.root,
											((Hashtable) (page.get(positions.get(j)))).get(newString),
											table.arrPageLoc.get(locations.get(i)), strTableName);

								Hashtable<String, Object> ptr = new Hashtable<>();
								ptr.put("key", htblColNameValue.get(newString));
								ptr.put("location", table.arrPageLoc.get(locations.get(i)));
								index2.root.id = (int) tableName_Column_Root.get(strTableName).get(newString + "id");
								index2.insert(index2.root, htblColNameValue.get(newString), ptr, strTableName,
										newString);
								tableName_Column_Root.get(strTableName).put(newString + "id", index2.root.id);
								tableName_Column_Root.get(strTableName).put(newString, index2.root);
							}
						}

						((Hashtable) page.get(positions.get(j))).replace(newString, htblColNameValue.get(newString));
						((Hashtable) page.get(positions.get(j))).replace("TouchDate", date);
					}

				}
				serializePage(page);
			}
		}

		serializeTCRFile(tableName_Column_Root);
		serializeTable(table);
		table = null;
		updatePolygon = false;
	}

	public static boolean compareCoordinates(Polygon p1, Polygon p2) {
		int p1X = p1.xpoints[0];
		int p1Y = p1.ypoints[0];
		int index = indexOf(p2.xpoints, p2.ypoints, p1X, p1Y);

		for (int i = 0; i < p1.xpoints.length; i++) {
			if (p1.xpoints[i] == p2.xpoints[index] && p1.ypoints[i] == p2.ypoints[index]) {
				index++;
				if (index == p2.xpoints.length)
					index = 0;
			} else
				return false;
		}

		return true;
	}

	public static int indexOf(int[] xpoints, int[] ypoints, int x, int y) {
		int index = 0;
		for (int i = 0; i < xpoints.length; i++) {
			if (x == xpoints[i] && y == ypoints[i]) {
				index = i;
				break;
			}

		}
		return index;
	}

	public int searchToInsertInNode(ArrayList<Object> keys, Object key, Table table) {

		if (!compare(key, (keys.get(0))))
			return 0;
		if (compare(key, (keys.get(keys.size() - 1))))
			return keys.size();

		return searchToInsertInNodeHelper(keys, 0, keys.size() - 1, key, table);
	}

	public int searchToInsertInNodeHelper(ArrayList<Object> keys, int l, int h, Object key, Table table) {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (compare(key, (keys.get(mid))) && compare((keys.get(mid + 1)), key))
				return mid + 1;
			// If element is smaller than mid, then it can only be present in left subarray
			if (compare((keys.get(mid)), key))
				return searchToInsertInNodeHelper(keys, l, mid - 1, key, table);
			// Else the element can only be present in right subarray
			return searchToInsertInNodeHelper(keys, mid + 1, h, key, table);
		}
		// We reach here when element is not present in array
		return -1;
	}

	public int findPageByIndex(Object key, Node root, String tableName, Table table)
			throws FileNotFoundException, DBAppException {
		Node leaf = search(tableName, root, key);
		int index = searchInNode(leaf, key);
		ArrayList<String> tuplePtrs;
		int pageLocation = -1;
		Page page;

		if (index == -1) {
			if (leaf.key.isEmpty()) {
				return -1;
			} else {
				index = searchToInsertInNode(leaf.key, key, table);
				if (index == leaf.key.size()) {
					if (leaf.rightpointer != null) {
						Node right = (leaf.rightpointer);
						tuplePtrs = right.tuplesPtr.get(0);
						tuplePtrs = sortString(tuplePtrs);
						pageLocation = table.arrPageLoc.indexOf(tuplePtrs.get(0));
					} else {
						page = deserializePage(table.arrPageLoc.get(table.arrPageLoc.size() - 1));
						if (page.checker())
							pageLocation = table.arrPageLoc.indexOf(table.arrPageLoc.get(table.arrPageLoc.size() - 1));
						else
							pageLocation = -1;
					}
				} else {
					tuplePtrs = leaf.tuplesPtr.get(index);
					tuplePtrs = sortString(tuplePtrs);
					pageLocation = table.arrPageLoc.indexOf(tuplePtrs.get(0));

				}
			}
		} else {
			if (index == leaf.key.size() - 1) {
				if (leaf.rightpointer != null) {
					Node right = (leaf.rightpointer);
					tuplePtrs = right.tuplesPtr.get(0);
					tuplePtrs = sortString(tuplePtrs);
					pageLocation = table.arrPageLoc.indexOf(tuplePtrs.get(0));
				} else {
					page = deserializePage(table.arrPageLoc.get(table.arrPageLoc.size() - 1));
					if (page.checker())
						pageLocation = table.arrPageLoc.indexOf(table.arrPageLoc.get(table.arrPageLoc.size() - 1));
					else
						pageLocation = -1;
				}
			} else {
				index++;
				tuplePtrs = leaf.tuplesPtr.get(index);
				tuplePtrs = sortString(tuplePtrs);
				pageLocation = table.arrPageLoc.indexOf(tuplePtrs.get(0));
			}
		}
		return pageLocation;

	}

	public static void serializNode(Node node) throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream(
				"data/" + node.Tablename + "_" + node.Columnname + "_" + node.Nodenumber + ".ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(node);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public Hashtable<String, Object> binarySearch(ArrayList<Object> arrayList, int l, int h, Object key) {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (equals(((Hashtable) (arrayList.get(mid))).get("key"), key))
				return (Hashtable<String, Object>) arrayList.get(mid);
			// If element is smaller than mid, then it can only be present in left subarray
			if (compare(((Hashtable) (arrayList.get(mid))).get("key"), key))
				return binarySearch(arrayList, l, mid - 1, key);
			// Else the element can only be present in right subarray
			return binarySearch(arrayList, mid + 1, h, key);
		}
		// We reach here when element is not present in array
		return null;
	}

	public int searchPage(Table table, ArrayList<Object> arraylist, Object key, ArrayList<String> arrPageLoc)
			throws FileNotFoundException, DBAppException {
		if (arraylist.isEmpty())
			return -1;

		if (compare(arraylist.get(0), key) && !(equals(key, (arraylist.get(0)))))
			return 0;

		if (compare(key, arraylist.get(arraylist.size() - 1))) {
			Page page = deserializePage(arrPageLoc.get((arraylist.size() - 1) / 2));
			if (page.checker()) {
				serializePage(page);
				return (arraylist.size() - 1) / 2;
			} else
				return -1;

		}
		return searchPageHelper(arraylist, 0, arraylist.size() - 1, key, arrPageLoc);
	}

	public int searchPageHelper(ArrayList<Object> arrayList, int l, int h, Object key, ArrayList<String> arrPageLoc)
			throws FileNotFoundException, DBAppException {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (compare(key, (arrayList.get(mid))) && compare((arrayList.get(mid + 1)), key)) {
				if (mid % 2 == 0) {
					/// new
					Page page = deserializePage(arrPageLoc.get(mid / 2));
					if (equals(key, (arrayList.get(mid + 1))) && !page.checker())
						return mid / 2 + 1;
					else
						return mid / 2;
				} else {
					Page page = deserializePage(arrPageLoc.get(mid / 2));
					if (page.checker()) {
						return mid / 2;
					} else
						return mid / 2 + 1;
				}

			} else if (compare(arrayList.get(mid), key)) {
				return searchPageHelper(arrayList, l, mid, key, arrPageLoc);
			} else {
				return searchPageHelper(arrayList, mid + 1, h, key, arrPageLoc);

			}
		}
		return 0;

	}

	public int searchToInsertInPage(Page page, Object key, Table table) {

		if (!compare(key, ((Hashtable) (page.get(0))).get(table.primarykey)))
			return 0;
		if (compare(key, ((Hashtable) (page.get(page.size() - 1))).get(table.primarykey)))
			return page.size();

		return searchToInsertInPageHelper(page, 0, page.size() - 1, key, table);
	}

	public int searchToInsertInPageHelper(Page page, int l, int h, Object key, Table table) {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (compare(key, ((Hashtable) (page.get(mid))).get(table.primarykey))
					&& compare(((Hashtable) (page.get(mid + 1))).get(table.primarykey), key))
				return mid + 1;
			// If element is smaller than mid, then it can only be present in left subarray
			if (compare(((Hashtable) (page.get(mid))).get(table.primarykey), key))
				return searchToInsertInPageHelper(page, l, mid - 1, key, table);
			// Else the element can only be present in right subarray
			return searchToInsertInPageHelper(page, mid + 1, h, key, table);
		}
		// We reach here when element is not present in array
		return -1;
	}

	//// test it
	public ArrayList<Integer> searchBinaryToDeleteFromPage(Page page, Object key, Table table, String clusterKey)
			throws DBAppException {
		if (key instanceof Polygon) {
			ArrayList<Integer> positions = searchBinaryToDeleteFromPageHelper(page, key,
					searchKeyInPage(key, page, table), table);
			ArrayList<Integer> result = new ArrayList<>();
			polygonSelect = true;
			DBApp.updatePolygon = false;
			for (int i = 0; i < positions.size(); i++) {
				if (equals(key, ((Hashtable) (page.get(positions.get(i)))).get(clusterKey)))
					result.add(positions.get(i));
			}
			DBApp.updatePolygon = true;
			polygonSelect = false;
			return result;
		} else
			return searchBinaryToDeleteFromPageHelper(page, key, searchKeyInPage(key, page, table), table);
	}

	private ArrayList<Integer> searchBinaryToDeleteFromPageHelper(Page page, Object key, int index, Table table)
			throws DBAppException {
		updatePolygon = false;
		ArrayList<Integer> arrayList = new ArrayList<>();
		if (index == -1 && key instanceof Polygon)
			return arrayList;
		if (index == -1)
			throw new DBAppException("There is no such record in this table");

		if (index > 0) {
			int leftindex = index - 1;

			while (equals(key, (((Hashtable) (page.get(leftindex))).get(table.primarykey)))) {
				arrayList.add(leftindex);
				leftindex--;
				if (leftindex == -1)
					break;
			}
		}
		if (index <= page.size() - 1) {
			while (equals(key, (((Hashtable) (page.get(index))).get(table.primarykey)))) {
				arrayList.add(index);
				index++;
				if (index == page.size())
					break;
			}
		}

		return arrayList;
	}

	public ArrayList<Integer> searchBinaryToSelectFromPage(Page page, Object key, Table table) throws DBAppException {
		return searchBinaryToSelectFromPageHelper(page, key, searchKeyInPage(key, page, table), table);
	}

	private ArrayList<Integer> searchBinaryToSelectFromPageHelper(Page page, Object key, int index, Table table)
			throws DBAppException {
		if (index == -1) {
			ArrayList<Integer> integers = new ArrayList<>();
			return integers;

		}

		ArrayList<Integer> arrayList = new ArrayList<>();
		if (index > 0) {
			int leftindex = index - 1;

			while (equals(key, (((Hashtable) (page.get(leftindex))).get(table.primarykey)))) {
				arrayList.add(leftindex);
				leftindex--;
				if (leftindex == -1)
					break;
			}
		}
		if (index <= page.size() - 1) {
			while (equals(key, (((Hashtable) (page.get(index))).get(table.primarykey)))) {
				arrayList.add(index);
				index++;
				if (index == page.size())
					break;
			}
		}

		return arrayList;
	}

	public ArrayList<Integer> searchBinaryToSelectFromPage1(Page page, Object key, Table table) throws DBAppException {
		return searchBinaryToSelectFromPageHelper1(page, key, searchKeyInPage1(key, page, table), table);
	}

	private ArrayList<Integer> searchBinaryToSelectFromPageHelper1(Page page, Object key, int index, Table table)
			throws DBAppException {
		if (index == -1) {
			ArrayList<Integer> integers = new ArrayList<>();
			return integers;

		}

		ArrayList<Integer> arrayList = new ArrayList<>();
		if (index > 0) {
			int leftindex = index - 1;

			while (equals(key, (((Hashtable) (page.get(leftindex))).get(table.primarykey)))) {
				arrayList.add(leftindex);
				leftindex--;
				if (leftindex == -1)
					break;
			}
		}
		if (index <= page.size() - 1) {
			while (equals(key, (((Hashtable) (page.get(index))).get(table.primarykey)))) {
				arrayList.add(index);
				index++;
				if (index == page.size())
					break;
			}
		}

		return arrayList;
	}

	public static ArrayList<Integer> sortPositions(ArrayList<Integer> arrayList) {
		int n = arrayList.size();
		for (int i = 1; i < n; ++i) {
			int key = arrayList.get(i);
			int j = i - 1;

			/*
			 * Move elements of arr[0..i-1], that are greater than key, to one position
			 * ahead of their current position
			 */
			while (j >= 0 && arrayList.get(j) > key) {
				arrayList.set(j + 1, arrayList.get(j));
				j = j - 1;
			}
			arrayList.set(j + 1, key);
		}

		return arrayList;
	}

	public int searchKeyInPage(Object key, Page page, Table table) {
		return searchKeyInPageHelper(key, page, 0, page.size() - 1, table);
	}

	private int searchKeyInPageHelper(Object key, Page page, int l, int h, Table table) {
		if (h >= l) {
			int mid = l + (h - l) / 2;

			if (equals(((Hashtable) (page.get(mid))).get(table.primarykey), (key)))
				return mid;

			else if (compare(((Hashtable) (page.get(mid))).get(table.primarykey), key))
				return searchKeyInPageHelper(key, page, 0, mid - 1, table);

			else
				return searchKeyInPageHelper(key, page, mid + 1, h, table);
		}
		return -1;
	}

	public int searchKeyInPage1(Object key, Page page, Table table) {
		return searchKeyInPageHelper1(key, page, 0, page.size() - 1, table);
	}

	private int searchKeyInPageHelper1(Object key, Page page, int l, int h, Table table) {
		if (h >= l) {
			int mid = l + (h - l) / 2;

			if (equals(((Hashtable) (page.get(mid))).get(table.primarykey), (key))) {
				polygonSelect = true;
				return mid;
			}

			else if (compare(((Hashtable) (page.get(mid))).get(table.primarykey), key))
				return searchKeyInPageHelper1(key, page, 0, mid - 1, table);

			else
				return searchKeyInPageHelper1(key, page, mid + 1, h, table);
		}
		return -1;
	}

	public ArrayList<String> searchPagesSelect(Object key, ArrayList<Object> minMaxKeys, Table table, String operator)
			throws DBAppException {

		ArrayList<String> locations = new ArrayList<>();
		if (minMaxKeys.size() == 0)
			return locations;
		if (!compare(key, minMaxKeys.get(0))) {
			locations.add("Key<Min");
			return locations;

		}
		if (key instanceof Polygon && !compare(minMaxKeys.get(minMaxKeys.size() - 1), key)) {
			locations.add("Key>Max");
			return locations;
		} else if (!(key instanceof Polygon) && !compare(minMaxKeys.get(minMaxKeys.size() - 1), key)) {
			locations.add("Key>Max");
			return locations;
		}
		if (key instanceof Polygon)
			return filterPolygons1(searchPagesSelectHelper(0, minMaxKeys.size() - 1, key, minMaxKeys, locations, table),
					table, key);
		else
			return searchPagesSelectHelper(0, minMaxKeys.size() - 1, key, minMaxKeys, locations, table);

	}

	public ArrayList<String> searchPagesSelectHelper(int l, int h, Object key, ArrayList<Object> minMaxKeys,
			ArrayList<String> locations, Table table) {
		if (h >= l) {
			int mid = l + (h - l) / 2;

			// If the element is present at the middle itself
			if (compare(key, minMaxKeys.get(mid)) && compare(minMaxKeys.get(mid + 1), key)) {
				locations.add(table.arrPageLoc.get(mid / 2));

				int leftindex = mid;
				int rightindex = mid + 1;

				while (equals(key, (minMaxKeys.get(rightindex)))) {
					locations.add(table.arrPageLoc.get(rightindex / 2));
					rightindex++;
					if (rightindex == minMaxKeys.size())
						break;
				}
				while (equals(key, (minMaxKeys.get(leftindex)))) {
					locations.add(table.arrPageLoc.get(leftindex / 2));
					leftindex--;
					if (leftindex == -1)
						break;
				}

			} else {
				// If element is smaller than mid, then it can only be present in left subarray
				if (compare(minMaxKeys.get(mid), key))
					return searchPagesSelectHelper(l, mid - 1, key, minMaxKeys, locations, table);
				// Else the element can only be present in right subarray
				return searchPagesSelectHelper(mid + 1, h, key, minMaxKeys, locations, table);
			}
		}
		return locations;

	}

	// take key and MinMaxKeys and returns indeces of all pages this key is in
	public ArrayList<Integer> searchPages(Object key, ArrayList<Object> minMaxKeys, Table table) throws DBAppException {
		ArrayList<Integer> locations = new ArrayList<>();
		if (minMaxKeys.size() == 0)
			return locations;
		if (!compare(key, minMaxKeys.get(0)))
			return locations;
		if (key instanceof Polygon && !compare(minMaxKeys.get(minMaxKeys.size() - 1), key))
			return locations;
		if (compare(key, minMaxKeys.get(minMaxKeys.size() - 1))) {
			for (int i = 0; i < table.arrPageLoc.size(); i++)
				locations.add(i);
			return locations;
		}

		else if (!(key instanceof Polygon) && compare(key, minMaxKeys.get(minMaxKeys.size() - 1))
				&& !equals(key, (minMaxKeys.get(minMaxKeys.size() - 1))))
			return locations;
		if (key instanceof Polygon)
			return filterPolygons2(searchPagesHelper(0, minMaxKeys.size() - 1, key, minMaxKeys, locations), table, key);
		else
			return searchPagesHelper(0, minMaxKeys.size() - 1, key, minMaxKeys, locations);

	}

	private ArrayList<Integer> filterPolygons2(ArrayList<Integer> array, Table table, Object key) {
		ArrayList<Integer> results = new ArrayList<>();
		Page page;
		updatePolygon = true;
		for (int i = 0; i < array.size(); i++) {
			page = deserializePage(table.arrPageLoc.get(array.get(i)));
			for (int j = 0; j < page.size(); j++) {
				if (equals(key, ((Hashtable) (page.get(j))).get(table.primarykey))) {
					results.add(array.get(i));
					break;
				}
			}
		}
		return results;
	}

	private ArrayList<Integer> filterPolygons(ArrayList<Integer> searchPagesHelper, Table table, Object key)
			throws DBAppException {
		polygonSelect = true;
		ArrayList<Integer> result = new ArrayList<>();
		searchPagesHelper = distinctInteger(searchPagesHelper);
		Page page;
		for (int i = 0; i < searchPagesHelper.size(); i++) {
			page = deserializePage(table.arrPageLoc.get(searchPagesHelper.get(i)));
			if (searchBinaryToDeleteFromPage(page, key, table, table.primarykey).size() > 0)
				result.add(searchPagesHelper.get(i));
		}
		return result;
	}

	private ArrayList<String> filterPolygons1(ArrayList<String> searchPagesHelper, Table table, Object key)
			throws DBAppException {
		ArrayList<String> result = new ArrayList<>();
		searchPagesHelper = distinctString(searchPagesHelper);
		Page page;
		for (int i = 0; i < searchPagesHelper.size(); i++) {
			page = deserializePage(searchPagesHelper.get(i));
			if (searchBinaryToDeleteFromPage(page, key, table, table.primarykey).size() > 0)
				result.add(searchPagesHelper.get(i));
		}
		return result;
	}

	public ArrayList<Integer> searchPagesHelper(int l, int h, Object key, ArrayList<Object> minMaxKeys,
			ArrayList<Integer> locations) {
		if (h >= l) {
			int mid = l + (h - l) / 2;

			// If the element is present at the middle itself
			if (compare(key, minMaxKeys.get(mid)) && compare(minMaxKeys.get(mid + 1), key)) {
				locations.add(mid / 2);

				int leftindex = mid;
				int rightindex = mid + 1;

				while (equals(key, (minMaxKeys.get(rightindex)))) {
					locations.add(rightindex / 2);
					rightindex++;
					if (rightindex == minMaxKeys.size())
						break;
				}
				while (equals(key, (minMaxKeys.get(leftindex)))) {
					locations.add(leftindex / 2);
					leftindex--;
					if (leftindex == -1)
						break;
				}

			} else {
				// If element is smaller than mid, then it can only be present in left subarray
				if (compare(minMaxKeys.get(mid), key))
					return searchPagesHelper(l, mid - 1, key, minMaxKeys, locations);
				// Else the element can only be present in right subarray
				return searchPagesHelper(mid + 1, h, key, minMaxKeys, locations);
			}
		}
		return locations;

	}

	public ArrayList<String> distinctString(ArrayList<String> arrayList) {
		ArrayList<String> distinct = new ArrayList<>();

		for (int i = 0; i < arrayList.size(); i++) {
			if (!distinct.contains(arrayList.get(i)))
				distinct.add(arrayList.get(i));
		}
		return distinct;
	}

	public ArrayList<Integer> distinctInteger(ArrayList<Integer> arrayList) {
		ArrayList<Integer> distinct = new ArrayList<>();

		for (int i = 0; i < arrayList.size(); i++) {
			if (!distinct.contains(arrayList.get(i)))
				distinct.add(arrayList.get(i));
		}
		return distinct;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException, FileNotFoundException {
		tableNames = readTableNames("data/TableNames.csv");
		metadataHash = deserializeFileMetaHash("data/metadataHash.ser");
		tableName_Column_Root = deserializeFileTCRHash("data/BTreesRTrees.ser");

		if (arrSQLTerms.length - 1 != strarrOperators.length)
			throw new DBAppException("Please enter correct query!");

		for (int i = 0; i < tableNames.size(); i++) {
			if (!tableNames.get(i).equals(arrSQLTerms[0].strTableName) && i == tableNames.size() - 1)
				throw new DBAppException("There is no such table in database");
		}

		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (metadataHash.get(arrSQLTerms[i].strTableName).get(arrSQLTerms[i].strColumnName) == null)
				throw new DBAppException("You have entered an extra column!");

			if (!metadataHash.get(arrSQLTerms[i].strTableName).get(arrSQLTerms[i].strColumnName).toLowerCase()
					.equals(arrSQLTerms[i].objValue.getClass().getName().toLowerCase()))
				throw new DBAppException("Please enter correct datatypes!");

			if (!arrSQLTerms[i].strOperator.equals("=") && !arrSQLTerms[i].strOperator.equals("!=")
					&& !arrSQLTerms[i].strOperator.equals(">=") && !arrSQLTerms[i].strOperator.equals(">")
					&& !arrSQLTerms[i].strOperator.equals("<=") && !arrSQLTerms[i].strOperator.equals("<"))
				throw new DBAppException("Please enter allowed operators inside SQLTERM!");
		}

		for (int i = 0; i < strarrOperators.length; i++) {
			if (!strarrOperators[i].equals("AND") && !strarrOperators[i].equals("OR")
					&& !strarrOperators[i].equals("XOR"))
				throw new DBAppException("Please enter allowed operators between SQLTERM!");
		}

		polygonCompare = "select";
		ArrayList<Hashtable<String, Object>> results;
		Table table = deserializeTable(arrSQLTerms[0].strTableName);

		results = selectfromTableHelper(arrSQLTerms, strarrOperators,
				tableName_Column_Root.get(arrSQLTerms[0].strTableName), table.primarykey, table, arrSQLTerms,
				strarrOperators);
		if (results.size() > 0) {
			if (results.get(results.size() - 1).get("7107107$") != null)
				results.remove(results.size() - 1);

		}
		polygonCompare = "";

		return results.iterator();

	}

	public static double areaPolygon(Polygon polygon) {
		return (((Polygon) polygon).getBounds().getSize().width) * (((Polygon) polygon).getBounds().getSize().height);
	}

	private ArrayList<Hashtable<String, Object>> selectfromTableHelper(SQLTerm[] arrSQLTerms, String[] strarrOperators,
			Hashtable<String, Object> res, String primary, Table table, SQLTerm[] arrSQLTermsfull,
			String[] strarrOperatorsfull) throws DBAppException, FileNotFoundException {

		ArrayList<Hashtable<String, Object>> results = new ArrayList<Hashtable<String, Object>>();
		if (strarrOperators.length == 0) {

			if (res.get(arrSQLTerms[0].strColumnName) != null) {
				BPTreeIndex index = new BPTreeIndex(arrSQLTerms[0].strTableName);
				index.root = (Node) res.get(arrSQLTerms[0].strColumnName);
				if (arrSQLTerms[0].strOperator.equals("=")) {
					// getting all page locations such that each page has at least one tuple
					// satisfying the condition
					polygonSelect = false;
					ArrayList<String> locations = index.searchForLocation1(arrSQLTerms[0].strTableName,
							(Node) res.get(arrSQLTerms[0].strColumnName), arrSQLTerms[0].objValue);
					polygonSelect = true;
					ArrayList<String> disLocations = distinctString(locations);
					// cluster and index
					if (arrSQLTerms[0].strColumnName.equals(primary)) {
						ArrayList<Integer> positions = new ArrayList<>();
						Page page;
						for (int i = 0; i < disLocations.size(); i++) {
							page = deserializePage(disLocations.get(i));
							positions = searchBinaryToDeleteFromPage(page, arrSQLTerms[0].objValue, table,
									table.primarykey);
							positions = sortPositions(positions);
							for (int j = positions.size() - 1; j >= 0; j--) {
								results.add((Hashtable<String, Object>) page.get(positions.get(j)));
							}
						}
					} else { // NonClusteredIndex
						Page page;
						// loop linearly (as the key is non-cluster one) in those page locations to find
						// tuples satisfying condition
						for (int i = 0; i < disLocations.size(); i++) {
							page = deserializePage(disLocations.get(i));
							for (int j = 0; j < page.size(); j++) {
								polygonSelect = true;
								if (equals(((Hashtable) (page.get(j))).get(arrSQLTerms[0].strColumnName),
										arrSQLTerms[0].objValue)) {
									results.add((Hashtable<String, Object>) page.get(j));
								}
							}
						}

					}

				} else if (arrSQLTerms[0].strOperator.equals(">")) {
					polygonSelect = false;
					if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
						throw new DBAppException("You can't use this datatype on this operator");
					// getting all page locations such that each page has at least one tuple
					// satisfying the condition
					ArrayList<String> location = locationsgreaterthan(arrSQLTerms[0].strTableName,
							(Node) res.get(arrSQLTerms[0].strColumnName), arrSQLTerms[0].objValue);
					ArrayList<String> dislocation = distinctString(location);
					dislocation = sortString(dislocation);
					// cluster and index
					if (arrSQLTerms[0].strColumnName.equals(primary)) {
//						ArrayList<Integer> indecesOfPages = searchPages(arrSQLTerms[0].objValue, table.MinMaxKeys,
//								table);
//						indecesOfPages = sortPositions(indecesOfPages);
//						indecesOfPages = distinctInteger(indecesOfPages);
//						if (indecesOfPages.size() == 0)
//							return results;
//						int maxpage = indecesOfPages.get(indecesOfPages.size() - 1);
						if (location.size() == 0)
							return results;
						String minPage = location.get(0);
						int min = table.arrPageLoc.indexOf(minPage);
						Page page1 = deserializePage(minPage);
						ArrayList<Integer> positions = searchBinaryToSelectFromPage(page1, arrSQLTerms[0].objValue,
								table);
						positions = sortPositions(positions);
						if (positions.size() > 0) {
							int maxinpage = positions.get(positions.size() - 1);
							if (maxinpage < page1.size() - 1) {
								page1 = deserializePage(minPage);

								results.addAll(page1.subList(maxinpage + 1, page1.size()));
							}
						} else {
							int position = searchToInsertInPage(page1, arrSQLTerms[0].objValue, table);
							results.addAll(page1.subList(position, page1.size()));
						}

						for (int i = min + 1; i < table.arrPageLoc.size(); i++) {
							page1 = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page1);
						}

					} else { // NonClusteredIndex
						Page page;
						// loop linearly (as the key is non-cluster one) in those page locations to find
						// tuples satisfying condition
						for (int i = 0; i < dislocation.size(); i++) {
							page = deserializePage(dislocation.get(i));
							for (int j = 0; j < page.size(); j++) {
								if (!compare(arrSQLTerms[0].objValue,
										((Hashtable) (page.get(j))).get(arrSQLTerms[0].strColumnName))) {
									results.add((Hashtable<String, Object>) page.get(j));
								}
							}
						}
					}

				} else if (arrSQLTerms[0].strOperator.equals(">=")) {
					polygonSelect = false;
					if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
						throw new DBAppException("You can't use this datatype on this operator");
					// getting all page locations such that each page has at least one tuple
					// satisfying the condition
					ArrayList<String> location = locationsgreaterthanequal(arrSQLTerms[0].strTableName,
							(Node) res.get(arrSQLTerms[0].strColumnName), arrSQLTerms[0].objValue);
					ArrayList<String> dislocation = distinctString(location);
					dislocation = sortString(dislocation);
					// cluster and index
					if (arrSQLTerms[0].strColumnName.equals(primary)) {
//						ArrayList<Integer> indecesOfPages = searchPages(arrSQLTerms[0].objValue, table.MinMaxKeys,
//								table);
//						indecesOfPages = sortPositions(indecesOfPages);
//						indecesOfPages = distinctInteger(indecesOfPages);
						if (location.isEmpty())
							return results;

						String minpage = location.get(0);
						int min = table.arrPageLoc.indexOf(minpage);
						Page page = deserializePage(minpage);
						ArrayList<Integer> positions = searchBinaryToSelectFromPage(page, arrSQLTerms[0].objValue,
								table);
						positions = sortPositions(positions);

						if (positions.size() > 0) {
							int mininpage = positions.get(0);
							results.addAll(page.subList(mininpage, page.size()));
						} else {
							int position = searchToInsertInPage(page, arrSQLTerms[0].objValue, table);
							results.addAll(page.subList(position, page.size()));
						}
						for (int i = min + 1; i < table.arrPageLoc.size(); i++) {
							page = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page);
						}
					} else { // NonClusteredIndex
						Page page;
						// loop linearly (as the key is non-cluster one) in those page locations to find
						// tuples satisfying condition
						for (int i = 0; i < dislocation.size(); i++) {
							page = deserializePage(dislocation.get(i));
							for (int j = 0; j < page.size(); j++) {
								if (compare(((Hashtable) (page.get(j))).get(arrSQLTerms[0].strColumnName),
										arrSQLTerms[0].objValue)) {
									results.add((Hashtable<String, Object>) page.get(j));
								}
							}
						}
					}
				} else if (arrSQLTerms[0].strOperator.equals("<")) {
					polygonSelect = false;
					if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
						throw new DBAppException("You can't use this datatype on this operator");
					// getting all page locations such that each page has at least one tuple
					// satisfying the condition
					ArrayList<String> location = locationslessthan(arrSQLTerms[0].strTableName,
							(Node) res.get(arrSQLTerms[0].strColumnName), arrSQLTerms[0].objValue);
					ArrayList<String> dislocation = distinctString(location);
					dislocation = sortString(dislocation);
					// cluster and index
					if (arrSQLTerms[0].strColumnName.equals(primary)) {
//						ArrayList<Integer> indecesOfPages = searchPages(arrSQLTerms[0].objValue, table.MinMaxKeys,
//								table);
//						indecesOfPages = sortPositions(indecesOfPages);
//						indecesOfPages = distinctInteger(indecesOfPages);
						if (location.isEmpty())
							return results;
						String maxPage = location.get(location.size() - 1);
						int max = table.arrPageLoc.indexOf(maxPage);
						Page page = deserializePage(maxPage);

						ArrayList<Integer> positions = searchBinaryToSelectFromPage(page, arrSQLTerms[0].objValue,
								table);
						positions = sortPositions(positions);

						for (int i = 0; i < max; i++) {
							page = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page);
						}

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page, arrSQLTerms[0].objValue, table);
							results.addAll(page.subList(0, position));

						} else {
							int mininpage = positions.get(0);
							results.addAll(page.subList(0, mininpage));
						}

					} else { // NonClusteredIndex
						Page page;
						// loop linearly (as the key is non-cluster one) in those page locations to find
						// tuples satisfying condition
						for (int i = 0; i < dislocation.size(); i++) {
							page = deserializePage(dislocation.get(i));
							for (int j = 0; j < page.size(); j++) {
								if (!compare(((Hashtable) (page.get(j))).get(arrSQLTerms[0].strColumnName),
										arrSQLTerms[0].objValue)) {
									results.add((Hashtable<String, Object>) page.get(j));
								}
							}
						}
					}
				} else if (arrSQLTerms[0].strOperator.equals("<=")) {
					polygonSelect = false;
					if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
						throw new DBAppException("You can't use this datatype on this operator");
					// getting all page locations such that each page has at least one tuple
					// satisfying the condition
					ArrayList<String> location = locationslessthanequal(arrSQLTerms[0].strTableName,
							(Node) res.get(arrSQLTerms[0].strColumnName), arrSQLTerms[0].objValue);
					ArrayList<String> dislocation = distinctString(location);
					dislocation = sortString(dislocation);
					// cluster and index
					if (arrSQLTerms[0].strColumnName.equals(primary)) {
//						ArrayList<Integer> indecesOfPages = searchPages(arrSQLTerms[0].objValue, table.MinMaxKeys,
//								table);
//						indecesOfPages = sortPositions(indecesOfPages);
//						indecesOfPages = distinctInteger(indecesOfPages);
						if (location.isEmpty()) {
							return results;
						}
						String maxpage = location.get(location.size() - 1);
						int max = table.arrPageLoc.indexOf(maxpage);
						Page page1 = deserializePage(maxpage);
						ArrayList<Integer> positions = searchBinaryToSelectFromPage(page1, arrSQLTerms[0].objValue,
								table);
						positions = sortPositions(positions);

						for (int i = 0; i < max; i++) {
							page1 = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page1);
						}

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page1, arrSQLTerms[0].objValue, table);
							results.addAll(page1.subList(0, position));
						} else {
							int maxinpage = positions.get(positions.size() - 1);
							results.addAll(page1.subList(0, maxinpage + 1));
						}

					} else { // NonClusteredIndex
						Page page;
						// loop linearly (as the key is non-cluster one) in those page locations to find
						// tuples satisfying condition
						for (int i = 0; i < dislocation.size(); i++) {
							page = deserializePage(dislocation.get(i));
							for (int j = 0; j < page.size(); j++) {
								if (compare(arrSQLTerms[0].objValue,
										((Hashtable) (page.get(j))).get(arrSQLTerms[0].strColumnName))) {
									results.add((Hashtable<String, Object>) page.get(j));
								}
							}
						}
					}

				} else {
					polygonSelect = true;
					results = notequal(index, arrSQLTerms[0].strTableName, index.root, arrSQLTerms[0].objValue, table);
				}

			} else { // no index on the column

				// clusterAndNonIndex
				if (arrSQLTerms[0].strColumnName.equals(primary)) {
					ArrayList<String> indecesOfPages;
					ArrayList<Integer> positions = new ArrayList<>();

					if (arrSQLTerms[0].strOperator.equals("=")) {
						polygonSelect = false;
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, "=");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);
						Page page;

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key<Min")
								|| indecesOfPages.get(0).equals("Key>Max"))
							return results;

						for (int i = 0; i < indecesOfPages.size(); i++) {
							page = deserializePage(indecesOfPages.get(i));
							positions = searchBinaryToSelectFromPage1(page, arrSQLTerms[0].objValue, table);
							positions = sortPositions(positions);
							for (int j = positions.size() - 1; j >= 0; j--) {
								results.add((Hashtable<String, Object>) page.get(positions.get(j)));
							}
						}
					}

					else if (arrSQLTerms[0].strOperator.equals(">")) {
						polygonSelect = false;
						if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
							throw new DBAppException("You can't use this datatype on this operator");
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, ">");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key>Max")) {
							return results;
						}

						if (indecesOfPages.get(0).equals("Key<Min")) {
							Page page;
							for (int i = 0; i < table.arrPageLoc.size(); i++) {
								page = deserializePage(table.arrPageLoc.get(i));
								results.addAll(page);
							}
							return results;
						}

						String maxPage = indecesOfPages.get(indecesOfPages.size() - 1);
						int max = table.arrPageLoc.indexOf(maxPage);
						Page page1 = deserializePage(maxPage);
						positions = searchBinaryToSelectFromPage(page1, arrSQLTerms[0].objValue, table);
						positions = sortPositions(positions);

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page1, arrSQLTerms[0].objValue, table);
							results.addAll(page1.subList(position, page1.size()));
						}

						else {
							int maxinpage = positions.get(positions.size() - 1);
							if (maxinpage < page1.size() - 1) {
								results.addAll(page1.subList(maxinpage + 1, page1.size()));

							}
						}
						for (int i = max + 1; i < table.arrPageLoc.size(); i++) {
							page1 = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page1);
						}
					} else if (arrSQLTerms[0].strOperator.equals(">=")) {
						polygonSelect = false;
						if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
							throw new DBAppException("You can't use this datatype on this operator");
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, ">=");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key>Max")) {
							return results;
						}

						if (indecesOfPages.get(0).equals("Key<Min")) {
							Page page;
							for (int i = 0; i < table.arrPageLoc.size(); i++) {
								page = deserializePage(table.arrPageLoc.get(i));
								results.addAll(page);
							}
							return results;
						}

						String minpage = indecesOfPages.get(0);
						int min = table.arrPageLoc.indexOf(minpage);
						Page page = deserializePage(minpage);

						positions = searchBinaryToSelectFromPage(page, arrSQLTerms[0].objValue, table);
						positions = sortPositions(positions);

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page, arrSQLTerms[0].objValue, table);
							results.addAll(page.subList(position, page.size()));
						} else {
							int mininpage = positions.get(0);
							results.addAll(page.subList(mininpage, page.size()));
						}
						for (int i = min + 1; i < table.arrPageLoc.size(); i++) {
							page = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page);
						}
					} else if (arrSQLTerms[0].strOperator.equals("<")) {
						polygonSelect = false;
						if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
							throw new DBAppException("You can't use this datatype on this operator");
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, "<");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key<Min")) {
							return results;
						}

						if (indecesOfPages.get(0).equals("Key>Max")) {
							Page page;
							for (int i = 0; i < table.arrPageLoc.size(); i++) {
								page = deserializePage(table.arrPageLoc.get(i));
								results.addAll(page);
							}
							return results;
						}

						String minpage = indecesOfPages.get(0);
						int min = table.arrPageLoc.indexOf(minpage);
						Page page = deserializePage(minpage);

						positions = searchBinaryToSelectFromPage(page, arrSQLTerms[0].objValue, table);
						positions = sortPositions(positions);

						Page page2;
						for (int i = 0; i < min; i++) {
							page2 = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page2);
						}

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page, arrSQLTerms[0].objValue, table);
							results.addAll(page.subList(0, position));
						} else {
							int mininpage = positions.get(0);
							results.addAll(page.subList(0, mininpage));

						}

					} else if (arrSQLTerms[0].strOperator.equals("<=")) {
						polygonSelect = false;
						if (arrSQLTerms[0].objValue.getClass().getName().equals("java.lang.Boolean"))
							throw new DBAppException("You can't use this datatype on this operator");
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, "<=");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key<Min")) {
							return results;
						}

						if (indecesOfPages.get(0).equals("Key>Max")) {
							Page page;
							for (int i = 0; i < table.arrPageLoc.size(); i++) {
								page = deserializePage(table.arrPageLoc.get(i));
								results.addAll(page);
							}
							return results;
						}

						String maxpage = indecesOfPages.get(indecesOfPages.size() - 1);
						int max = table.arrPageLoc.indexOf(maxpage);

						Page page1 = deserializePage(maxpage);
						positions = searchBinaryToSelectFromPage(page1, arrSQLTerms[0].objValue, table);
						positions = sortPositions(positions);

						Page page;
						for (int i = 0; i < max; i++) {
							page = deserializePage(table.arrPageLoc.get(i));
							results.addAll(page);
						}

						if (positions.isEmpty()) {
							int position = searchToInsertInPage(page1, arrSQLTerms[0].objValue, table);
							results.addAll(page1.subList(0, position));
						} else {
							int maxinpage = positions.get(positions.size() - 1);
							results.addAll(page1.subList(0, maxinpage + 1));
						}

					} else {
						// !=
						polygonSelect = false;
						indecesOfPages = searchPagesSelect(arrSQLTerms[0].objValue, table.MinMaxKeys, table, "!=");
						indecesOfPages = distinctString(indecesOfPages);
						indecesOfPages = sortString(indecesOfPages);

						if (indecesOfPages.isEmpty() || indecesOfPages.get(0).equals("Key<Min")
								|| indecesOfPages.get(0).equals("Key>Max")) {
							Page page;
							for (int i = 0; i < table.arrPageLoc.size(); i++) {
								page = deserializePage(table.arrPageLoc.get(i));
								results.addAll(page);
							}
							return results;
						}

						ArrayList<String> temp = table.arrPageLoc;
						Page page;
						for (int i = 0; i < temp.size(); i++) {
							if (indecesOfPages.contains(temp.get(i))) {
								page = deserializePage(temp.get(i));

								if (metadataHash.get(arrSQLTerms[0].strTableName).get(arrSQLTerms[0].strColumnName)
										.toLowerCase().equals("java.awt.polygon")) {
									for (int k = 0; k < page.size(); k++) {
										polygonSelect = true;
										if (!equals(arrSQLTerms[0].objValue,
												((Hashtable) (page.get(k))).get(arrSQLTerms[0].strColumnName)))
											results.add((Hashtable) page.get(k));
									}

								} else {
									positions = searchBinaryToSelectFromPage(page, arrSQLTerms[0].objValue, table);
									positions = sortPositions(positions);

									for (int k = positions.size() - 1; k >= 0; k--) {
										page.remove(positions.get(k));
									}
									results.addAll(page);
								}

							} else {
								page = deserializePage(temp.get(i));
								results.addAll(page);

							}
						}

					}
				} else {// noncluster nonindex linearly
					Page p;
					for (int i = 0; i < table.arrPageLoc.size(); i++) {
						p = deserializePage(table.arrPageLoc.get(i));
						for (int j = 0; j < p.size(); j++) {
							if (condition((Hashtable<String, Object>) p.get(j), arrSQLTermsfull, strarrOperatorsfull))
								results.add((Hashtable<String, Object>) p.get(j));
						}
					}
					Hashtable<String, Object> r = new Hashtable<String, Object>();
					r.put("7107107$", "ZAEKH");
					results.add(r);

				}

			}
			return results;
		} else {
			if (strarrOperators[strarrOperators.length - 1].equals("AND")) { // filtering without recursive index or
																				// cluster
				if ((res.get(arrSQLTerms[arrSQLTerms.length - 1].strColumnName) != null)
						|| (arrSQLTerms[arrSQLTerms.length - 1].strColumnName.equals(primary))) {
					SQLTerm[] x = new SQLTerm[1];
					x[0] = arrSQLTerms[arrSQLTerms.length - 1];
					ArrayList<Hashtable<String, Object>> tempresult = selectfromTableHelper(x, new String[0], res,
							primary, table, arrSQLTermsfull, strarrOperatorsfull);
					for (int i = 0; i < tempresult.size(); i++) {
						if (condition(tempresult.get(i), arrSQLTermsfull, strarrOperatorsfull))
							results.add(tempresult.get(i));

					}
					return results;
				} else {

					SQLTerm[] y = new SQLTerm[arrSQLTerms.length - 1];

					for (int i = 0; i < y.length; i++)
						y[i] = arrSQLTerms[i];

					String[] z = new String[strarrOperators.length - 1];

					for (int i = 0; i < z.length; i++)
						z[i] = strarrOperators[i];

					ArrayList<Hashtable<String, Object>> tempresult = selectfromTableHelper(y, z, res, primary, table,
							arrSQLTermsfull, strarrOperatorsfull);
					if (tempresult.size() > 0) {
						if (tempresult.get(tempresult.size() - 1).get("7107107$") != null)
							return tempresult;
					}

					for (int i = 0; i < tempresult.size(); i++) {
						String[] o = new String[1];
						o[0] = "AND";
						SQLTerm[] e = new SQLTerm[1];
						e[0] = arrSQLTerms[arrSQLTerms.length - 1];
						if (condition(tempresult.get(i), e, o))
							results.add(tempresult.get(i));

					}
					return results;

				}

			} else {
				if (!((res.get(arrSQLTerms[arrSQLTerms.length - 1].strColumnName) != null)
						|| (arrSQLTerms[arrSQLTerms.length - 1].strColumnName.equals(primary)))) {
					SQLTerm[] x = new SQLTerm[1];
					x[0] = arrSQLTerms[arrSQLTerms.length - 1];
					return selectfromTableHelper(x, new String[0], res, primary, table, arrSQLTermsfull,
							strarrOperatorsfull);
				} else {
					SQLTerm[] y = new SQLTerm[arrSQLTerms.length - 1];

					for (int i = 0; i < y.length; i++)
						y[i] = arrSQLTerms[i];

					String[] z = new String[strarrOperators.length - 1];

					for (int i = 0; i < z.length; i++)
						z[i] = strarrOperators[i];
					ArrayList<Hashtable<String, Object>> tempresleft = selectfromTableHelper(y, z, res, primary, table,
							arrSQLTermsfull, strarrOperatorsfull);
					if (tempresleft.size() > 0) {
						if (tempresleft.get(tempresleft.size() - 1).get("7107107$") != null)
							return tempresleft;
					}

					SQLTerm[] x = new SQLTerm[1];
					x[0] = arrSQLTerms[arrSQLTerms.length - 1];
					String[] r = new String[1];
					r[0] = "OR";
					tempresleft = condition1(tempresleft, x, r);
					ArrayList<Hashtable<String, Object>> tempresright = selectfromTableHelper(x, new String[0], res,
							primary, table, arrSQLTermsfull, strarrOperatorsfull);
					if (tempresright.size() > 0) {
						if (tempresright.get(tempresright.size() - 1).get("7107107$") != null)
							return tempresright;
					}
					if (strarrOperators[strarrOperators.length - 1].equals("OR")) {
						results.addAll(tempresleft);
						results.addAll(tempresright);
						return results;
					} else {
						tempresright = condition1(tempresright, y, z);
						results.addAll(tempresleft);
						results.addAll(tempresright);
						return results;
					}
				}
			}
		}

	}

	public static boolean equals(Object x, Object y) {
		if (x instanceof Polygon) {
			switch (polygonCompare) {
			case "insert":
				int z = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
				int w = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
				if (z == w) {
					return true;
				} else
					return false;

			case "select":
				// polygonSelect = true? ----> =/!=
				if (polygonSelect) {
					return compareCoordinates((Polygon) x, (Polygon) y);
				} else {
					int z1 = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
					int w1 = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
					if (z1 == w1) {
						return true;
					} else
						return false;
				}
			default:
				if (updatePolygon) {
					int z1 = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
					int w1 = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
					if (z1 == w1) {
						return true;
					} else
						return false;
				} else
					return compareCoordinates((Polygon) x, (Polygon) y);
			}
		} else if (x instanceof Date) {
			int dayX = ((Date) x).getDate();
			int monthX = ((Date) x).getMonth();
			int yearX = ((Date) x).getYear();

			int dayY = ((Date) y).getDate();
			int monthY = ((Date) y).getMonth();
			int yearY = ((Date) y).getYear();

			if (yearX == yearY && monthX == monthY && dayX == dayY)
				return true;
			else
				return false;
		} else
			return x.equals(y);

	}

	private ArrayList<Hashtable<String, Object>> condition1(ArrayList<Hashtable<String, Object>> tempresleft,
			SQLTerm[] x, String[] r) {

		ArrayList<Hashtable<String, Object>> temp = new ArrayList<Hashtable<String, Object>>();
		for (int i = 0; i < tempresleft.size(); i++) {

			if (!condition(tempresleft.get(i), x, r))
				temp.add(tempresleft.get(i));
		}
		return temp;
	}

	private boolean condition(Hashtable<String, Object> tuple, SQLTerm[] arrSQLTermsfull,
			String[] strarrOperatorsfull) {
		int[] bits = new int[arrSQLTermsfull.length];
		Object value;
		for (int i = 0; i < arrSQLTermsfull.length; i++) {
			value = tuple.get(arrSQLTermsfull[i].strColumnName);
			switch (arrSQLTermsfull[i].strOperator) {
			case "=":
				polygonSelect = true;
				if (equals(value, arrSQLTermsfull[i].objValue))
					bits[i] = 1;
				else
					bits[i] = 0;
				break;
			case ">":
				polygonSelect = false;
				if (!compare(arrSQLTermsfull[i].objValue, value))
					bits[i] = 1;
				else
					bits[i] = 0;
				break;
			case ">=":
				polygonSelect = false;
				if (compare(value, arrSQLTermsfull[i].objValue))
					bits[i] = 1;
				else
					bits[i] = 0;
				break;
			case "<":
				polygonSelect = false;
				if (!compare(value, arrSQLTermsfull[i].objValue))
					bits[i] = 1;
				else
					bits[i] = 0;
				break;
			case "<=":
				polygonSelect = false;
				if (compare(arrSQLTermsfull[i].objValue, value))
					bits[i] = 1;
				else
					bits[i] = 0;
				break;

			default:
				polygonSelect = true;
				if (equals(value, arrSQLTermsfull[i].objValue))
					bits[i] = 0;
				else
					bits[i] = 1;
				break;
			}
		}
		if (arrSQLTermsfull.length == 1) {
			if (bits[0] == 1)
				return true;
			else
				return false;
		}

		return bitscomputation(bits, strarrOperatorsfull);

	}

	private boolean bitscomputation(int[] bits, String[] strarrOperatorsfull) {
		for (int i = 0; i < strarrOperatorsfull.length; i++) {
			if (strarrOperatorsfull[i].equals("AND"))
				bits[i + 1] = And(bits[i], bits[i + 1]);
			else if (strarrOperatorsfull[i].equals("OR"))
				bits[i + 1] = Or(bits[i], bits[i + 1]);
			else
				bits[i + 1] = XOR(bits[i], bits[i + 1]);

		}

		if (bits[bits.length - 1] == 1)
			return true;
		else
			return false;

	}

	private int And(int i, int j) {
		if (i == 1 && j == 1)
			return 1;
		else
			return 0;
	}

	private int Or(int i, int j) {
		if (i == 0 && j == 0)
			return 0;
		else
			return 1;
	}

	private int XOR(int i, int j) {
		if ((i == 0 && j == 1) || (i == 1 && j == 0))
			return 1;
		else
			return 0;
	}

	private static ArrayList<String> sortString(ArrayList<String> array) {
		Hashtable<String, Object> hashtable = new Hashtable<>();
		ArrayList<Integer> integers = new ArrayList<>();
		ArrayList<String> result = new ArrayList<>();

		for (int i = 0; i < array.size(); i++) {
			int value = extractPageNumber(array.get(i));
			hashtable.put(i + "string", array.get(i));
			hashtable.put(i + "value", value);
			hashtable.put(value + "", new ArrayList<>());
			integers.add(value);
			result.add("");
		}
		// System.out.println(integers);

		integers = sortPositions(integers);
		// System.out.println(integers);

		for (int i = 0; i < integers.size(); i++) {
			((ArrayList) (hashtable.get(integers.get(i) + ""))).add(i);
		}

		// System.out.println(result);
		// System.out.println(hashtable);
		for (int i = 0; i < array.size(); i++) {
			int value = (int) hashtable.get(i + "value");
			int index = (int) ((ArrayList) (hashtable.get(value + ""))).remove(0);
			String location = (String) hashtable.get(i + "string");
			result.set(index, location);
			// System.out.println(result);

		}

		return result;

	}

	private static int extractPageNumber(String string) {
		int value = 0;
		String temp = "";
		for (int i = 0; i < string.length(); i++) {
			try {
				Integer.parseInt(string.charAt(i) + "");
				for (int j = i; j < string.length(); j++) {
					temp += string.charAt(j);
					try {
						if (j == string.length() - 1)
							return Integer.parseInt(temp);
						else
							Integer.parseInt(string.charAt(j + 1) + "");
					} catch (Exception e) {
						// TODO: handle exception
						value = Integer.parseInt(temp);
						return value;
					}
				}

			} catch (Exception e) {

			}
		}
		return value;
	}

	public ArrayList<Hashtable<String, Object>> notequal(BPTreeIndex index, String tablename, Node root, Object key,
			Table table) throws DBAppException, FileNotFoundException {
		ArrayList<String> temp = table.arrPageLoc;
		ArrayList<Hashtable<String, Object>> tuple = new ArrayList<Hashtable<String, Object>>();
		polygonSelect = false;
		ArrayList<String> locations = index.searchForLocation1(tablename, root, key);
		ArrayList<String> disLocations = distinctString(locations);
		// cluster and index
		ArrayList<Integer> positions = new ArrayList<>();
		Page page;
		for (int i = 0; i < disLocations.size(); i++) {
			page = deserializePage(disLocations.get(i));
			polygonSelect = true;
			positions = searchBinaryToDeleteFromPage(page, key, table, table.primarykey);
			positions = sortPositions(positions);
			ArrayList<Integer> allpositions = new ArrayList<Integer>();
			for (int k = 0; k < page.size(); k++) {
				allpositions.add(k);
			}

			allpositions.removeAll(positions);
			for (int w = 0; w < allpositions.size(); w++) {
				tuple.add((Hashtable<String, Object>) page.get(allpositions.get(w)));
			}
		}

		temp.removeAll(disLocations);

		for (int i = 0; i < temp.size(); i++) {
			page = deserializePage(temp.get(i));
			tuple.addAll(page);

		}
		page = null;
		return tuple;

	}

	public ArrayList<String> locationsgreaterthanequal(String table, Node root, Object key)
			throws DBAppException, FileNotFoundException {
		ArrayList<String> tuple = new ArrayList<String>();
		Node leaf = search(table, root, key);
		int index = searchInNode(leaf, key);
		int c = 0;
		if (index == -1) {

			for (int i = 0; i < leaf.key.size(); i++) {
				if (compare(key, leaf.key.get(c)))
					c++;
				else
					break;
			}

			index = c;

		}

		for (int i = index; i < leaf.key.size(); i++) {
			tuple.addAll(leaf.tuplesPtr.get(i));

		}

		while (leaf.rightpointer != null) {
			leaf = (leaf.rightpointer);
			for (int i = 0; i < leaf.key.size(); i++) {
				tuple.addAll(leaf.tuplesPtr.get(i));

			}
		}
		leaf = null;
		root = null;
		return tuple;
	}

	public ArrayList<String> locationsgreaterthan(String table, Node root, Object key)
			throws DBAppException, FileNotFoundException {
		ArrayList<String> tuple = new ArrayList<String>();
		Node leaf = search(table, root, key);
		int index = searchInNode(leaf, key);
		int c = 0;
		if (index == -1) {

			for (int i = 0; i < leaf.key.size(); i++) {
				if (compare(key, leaf.key.get(c)))
					c++;
				else
					break;
			}

			index = c;
		} else
			index++;

		for (int i = index; i < leaf.key.size(); i++) {
			tuple.addAll(leaf.tuplesPtr.get(i));

		}

		while (leaf.rightpointer != null) {
			leaf = (leaf.rightpointer);
			for (int i = 0; i < leaf.key.size(); i++) {
				tuple.addAll(leaf.tuplesPtr.get(i));

			}
		}
		leaf = null;
		root = null;
		return tuple;
	}

	public Node smallestNode(Node root) {
		if (root.isLeaf) {
			return root;
		} else
			return smallestNode((root.ptr.get(0)));
	}

	public ArrayList<String> locationslessthanequal(String table, Node root, Object key)
			throws DBAppException, FileNotFoundException {
		ArrayList<String> tuple = new ArrayList<String>();
		Node leaf = search(table, root, key);
		int index = searchInNode(leaf, key);
		int c = 0;
		if (index == -1) {

			for (int i = 0; i < leaf.key.size(); i++) {
				if (compare(key, leaf.key.get(c)))
					c++;
				else
					break;
			}
		}
		Node node1 = smallestNode(root);
		if (index != -1) {
			if (node1.equals(leaf)) {
				for (int i = 0; i <= index; i++)
					tuple.addAll(node1.tuplesPtr.get(i));

			} else {

				while (!node1.rightpointer.equals(leaf)) {
					node1 = (node1.rightpointer);
					for (int i = 0; i < node1.key.size(); i++)
						tuple.addAll(node1.tuplesPtr.get(i));

				}

				for (int i = 0; i <= index; i++)

					tuple.addAll(leaf.tuplesPtr.get(i));
			}
		} else {
			if (node1.equals(leaf)) {
				for (int i = 0; i < c; i++)
					tuple.addAll(node1.tuplesPtr.get(i));

			} else {

				while (!node1.rightpointer.equals(leaf)) {
					node1 = (node1.rightpointer);
					for (int i = 0; i < node1.key.size(); i++)
						tuple.addAll(node1.tuplesPtr.get(i));

				}

				for (int i = 0; i < c; i++)

					tuple.addAll(leaf.tuplesPtr.get(i));
			}

		}
		node1 = null;
		leaf = null;
		root = null;
		return tuple;
	}

	public ArrayList<String> locationslessthan(String table, Node root, Object key)
			throws DBAppException, FileNotFoundException {
		ArrayList<String> tuple = new ArrayList<String>();
		Node leaf = search(table, root, key);
		int index = searchInNode(leaf, key);
		if (index == -1) {
			index = 0;
			for (int i = 0; i < leaf.key.size(); i++) {
				if (compare(key, leaf.key.get(index)))
					index++;
				else
					break;
			}
		}
		Node node1 = smallestNode(root);
		if (node1.equals(leaf)) {
			for (int i = 0; i < index; i++)
				tuple.addAll(node1.tuplesPtr.get(i));

		} else {
			while (!node1.rightpointer.equals(leaf)) {
				node1 = (node1.rightpointer);
				for (int i = 0; i < node1.key.size(); i++)
					tuple.addAll(node1.tuplesPtr.get(i));

			}

			for (int i = 0; i < index; i++)

				tuple.addAll(leaf.tuplesPtr.get(i));

		}
		node1 = null;
		leaf = null;
		root = null;
		return tuple;
	}

	public int searchInNode(Node node, Object key) {
		return searchInNodeHelper(node.key, 0, node.key.size() - 1, key);
	}

	public int searchInNodeHelper(ArrayList<Object> array, int l, int h, Object key) {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (equals(key, array.get(mid)))
				return mid;
			// If element is smaller than mid, then it can only be present in left subarray
			if (compare(array.get(mid), key))
				return searchInNodeHelper(array, l, mid - 1, key);
			// Else the element can only be present in right subarray
			return searchInNodeHelper(array, mid + 1, h, key);
		}
		// We reach here when element is not present in array
		return -1;
	}

	public Node search(String tableName, Node node, Object key) throws FileNotFoundException, DBAppException {
		if (node.isLeaf || node.key.size() == 0)
			return node;
		else {

			for (int i = 0; i < node.key.size(); i++) {
				if (compare(node.key.get(i), key) && !equals(key, node.key.get(i))) {
					return search(tableName, (node.ptr.get(i)), key);
				}
			}

			return search(tableName, (node.ptr.get(node.ptr.size() - 1)), key);

		}

	}

	/*
	 * public void serializeFile(ArrayList<ArrayList<Object>> array) throws
	 * DBAppException, FileNotFoundException { FileOutputStream fileOut = new
	 * FileOutputStream("data/BTreesRTrees.ser"); try {
	 * 
	 * ObjectOutputStream out = new ObjectOutputStream(fileOut);
	 * out.writeObject(array); out.close(); fileOut.close(); } catch (IOException i)
	 * { i.printStackTrace(); }
	 * 
	 * }
	 */

	public void serializeTCRFile(Hashtable<String, Hashtable<String, Object>> array)
			throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("data/BTreesRTrees.ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(array);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public void serializeMetaFile(Hashtable<String, Hashtable<String, String>> array)
			throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("data/metadataHash.ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(array);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	/*
	 * public static void serializeFile(Hashtable<String, Hashtable<String, Object>>
	 * metadataHash) throws DBAppException, FileNotFoundException { FileOutputStream
	 * fileOut = new FileOutputStream("data/metadataHash.ser"); try {
	 * 
	 * ObjectOutputStream out = new ObjectOutputStream(fileOut);
	 * out.writeObject(metadataHash); out.close(); fileOut.close(); } catch
	 * (IOException i) { i.printStackTrace(); }
	 * 
	 * }
	 */

	public void serializeTable(Table table) throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("data/" + table.name + ".ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public Table deserializeTable(String tableName) {
		Table p = null;
		try {
			FileInputStream fileIn = new FileInputStream("data/" + tableName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Table) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			c.printStackTrace();

		}
		return p;
	}

	public void serializePage(Page page) throws DBAppException, FileNotFoundException {
		FileOutputStream fileOut = new FileOutputStream("data/" + page.tableName + "_" + page.id + ".ser");
		try {

			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

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

	public Node deserializeNode(String NodeLoc) {
		Node N = null;
		try {
			FileInputStream fileIn = new FileInputStream(NodeLoc);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			N = (Node) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found");
			c.printStackTrace();

		}
		return N;
	}

	public static ArrayList<ArrayList<Object>> deserializeFile(String fileLocation) {
		ArrayList<ArrayList<Object>> p = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileLocation);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (ArrayList<ArrayList<Object>>) in.readObject();
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

	public static Hashtable<String, Hashtable<String, String>> deserializeFileMetaHash(String fileLocation) {
		Hashtable<String, Hashtable<String, String>> p = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileLocation);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Hashtable<String, Hashtable<String, String>>) in.readObject();
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

	public static Hashtable<String, Hashtable<String, Object>> deserializeFileTCRHash(String fileLocation) {
		Hashtable<String, Hashtable<String, Object>> p = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileLocation);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Hashtable<String, Hashtable<String, Object>>) in.readObject();
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

	public static String givenUsingJava8_whenGeneratingRandomAlphanumericString_thenCorrect() {
		int leftLimit = 48; // numeral '0'
		int rightLimit = 122; // letter 'z'
		int targetStringLength = 10;
		Random random = new Random();

		String generatedString = random.ints(leftLimit, rightLimit + 1)
				.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(targetStringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

		return generatedString;
	}

	public static void printPolygon(Polygon polygon) {
		for (int i = 0; i < polygon.xpoints.length; i++) {
			if (i == polygon.xpoints.length - 1)
				System.out.print("(" + polygon.xpoints[i] + "," + polygon.ypoints[i] + ")");
			else
				System.out.print("(" + polygon.xpoints[i] + "," + polygon.ypoints[i] + "),");
		}
		System.out.print(" Area: "
				+ (((Polygon) polygon).getBounds().getSize().width) * (((Polygon) polygon).getBounds().getSize().height)
				+ "   ");
	}
	public static void main(String[] args) {
		
	}
}
