package ZAEKH;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("serial")
class Node implements Serializable {
	public ArrayList<Object> key;
	public ArrayList<Node> ptr;
	public Node parent;
	public Node rightpointer;
	public String Tablename;
	public String Columnname;
	public int Nodenumber;
	public Node leftpointer;
	public boolean isLeaf;
	public ArrayList<ArrayList<String>> tuplesPtr;
	static int id = 0;
	protected int indexInParent;
	static int minKeysLeaf = (int) Math.floor(((loadProperty()) + 1) / 2);
	static int minKeysNonLeaf = (int) Math.ceil((double) (loadProperty() + 1) / (double) 2) - 1;

	public Node(String Tablename, String Columnname) {
		this.key = new ArrayList<Object>();
		this.ptr = new ArrayList<Node>();
		this.parent = null;
		this.rightpointer = null;
		this.Columnname = Columnname;
		this.Tablename = Tablename;
		this.leftpointer = null;
		this.isLeaf = false;
		this.id = createId();
		this.Nodenumber = this.id;
		tuplesPtr = new ArrayList<>();
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

	public static Node deserializeNode(String NodeLoc) {
		Node p = null;
		try {
			FileInputStream fileIn = new FileInputStream(NodeLoc);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Node) in.readObject();
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

	public int createId() {
		int d = id + 1;
		return d;
	}

	public static int loadProperty() {
		Properties prop = new Properties();

		try (InputStream input = new FileInputStream("config/DBApp.properties")) {

			// load a properties file
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return Integer.parseInt(prop.getProperty("NodeSize"));
	}

	public void setParent(Node parent) {

		this.parent = parent;
		for (int i = 0; i < parent.ptr.size(); i++) {

			if (parent.ptr.get(i).equals(this)) {
				this.indexInParent = i;
				break;
			}
		}
	}

	public boolean isUnderflowed() {
		return key.size() < minKeysLeaf;
	}

	public boolean isUnderflowedNonLeaf() {
		return key.size() < minKeysNonLeaf;
	}

	public int getIndexInParent() {
		return indexInParent;
	}

	public Node getParent() {
		return parent;
	}

	public static void main(String[] args) throws FileNotFoundException, DBAppException {

	}
}