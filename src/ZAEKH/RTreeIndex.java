package ZAEKH;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.function.IntConsumer;

public class RTreeIndex {
	static Node root;
	static int Nodesize = loadProperty() + 1;
	static String tableName;
	static Node searchNode;
	public int c = 0;
	static int count = 0;

	public RTreeIndex(String tableName) {
		this.tableName = tableName;
	}

	/*
	 * Insert function which inserts the record into the tree. It validates whether
	 * a record is already present in the index file and skips it. When the nodes
	 * inserted equals the node size of the tree, the split function is called and
	 * the tree is balanced.
	 */
	@SuppressWarnings("null")
	public static void insert(Node node, Object key, Hashtable<String, Object> ptr, String Tablename, String Columnname)
			throws IOException, DBAppException {
		//// the node is the root and with empty key
		if ((node == null || node.key.isEmpty()) && node == root) {
			node.key.add(key);
			node.isLeaf = true;
			ArrayList<String> arrayList = new ArrayList<>();
			arrayList.add((String) ptr.get("location"));
			node.tuplesPtr.add(arrayList);
			root = node;

			return;
		}
		//// root with at least 1 key or it is not root
		else if (node != null || !node.key.isEmpty()) {
			for (int i = 0; i < node.key.size(); i++) {
				/////////// overflow pages////////////
				if (equals(key, (node.key.get(i)))) {
					if (!node.isLeaf && node.ptr.get(i + 1) != null) {
						insert(node.ptr.get(i + 1), key, ptr, Tablename, Columnname);
						// serializNode(n);
						return;
					} else if (node.isLeaf) {
						node.tuplesPtr.get(i).add((String) ptr.get("location"));
						if (node.key.size() == Nodesize) {
							split(node, Tablename, Columnname);
							return;
						} else
							return;
					}

				} else if (compare(node.key.get(i), key)) {
					// node = deserializeNode("");
					if (!node.isLeaf && node.ptr.get(i) != null) {
						insert((node.ptr.get(i)), key, ptr, Tablename, Columnname);
						return;
					} else if (node.isLeaf) {
						node.key.add("");
						for (int j = node.key.size() - 2; j >= i; j--) {
							node.key.set(j + 1, node.key.get(j));
						}
						node.key.set(i, key);

						ArrayList<String> arrayList = new ArrayList<>();
						arrayList.add((String) ptr.get("location"));
						node.tuplesPtr.add(i, arrayList);

						if (node.key.size() == Nodesize) {
							split(node, Tablename, Columnname);
							return;
						} else
							return;
					}
				} else if (compare(key, node.key.get(i))) {
					if (i < node.key.size() - 1) {
						continue;
					} else if (i == node.key.size() - 1) {
						if (!node.isLeaf && node.ptr.get(i + 1) != null) {
							insert(node.ptr.get(i + 1), key, ptr, Tablename, Columnname);
							return;
						}

						else if (node.isLeaf) {
							node.key.add("");
							node.key.set(i + 1, key);
							ArrayList<String> arrayList = new ArrayList<>();
							arrayList.add((String) ptr.get("location"));
							node.tuplesPtr.add(arrayList);
						}

						if (node.key.size() == Nodesize) {
							split(node, Tablename, Columnname);
							return;
						} else
							return;
					}
				}
			}
		}
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

	/*
	 * This function splits the tree and balances the node in it. When the nodes
	 * inserted exceeds the node size this function is called After split it creates
	 * the pointer and stores the pointer of the parent, right node and left node to
	 * it if it is an internal node and if it is a leaf node stores the right
	 * pointer to the node. Before splitting the function sorts the key in ascending
	 * order and perform the split.
	 */
	public static void split(Node node, String Tablename, String Columnname) throws IOException, DBAppException {
		Node leftnode = new Node(Tablename, Columnname);
		Node rightnode = new Node(Tablename, Columnname);
		Node tempparent = new Node(Tablename, Columnname);

		Node parent;
		int newPosKey = 0, split = 0;

		if (node.isLeaf) {
			if (node.key.size() % 2 == 0)
				split = node.key.size() / 2;
			else
				split = (int) Math.ceil((double) (loadProperty() + 1) / (double) 2) - 1;

			rightnode.isLeaf = true;
			for (int i = split; i < node.key.size(); i++) {
				rightnode.key.add(node.key.get(i));
				rightnode.tuplesPtr.add(node.tuplesPtr.get(i));
			}

			leftnode.isLeaf = true;
			for (int i = 0; i < split; i++) {
				leftnode.key.add(node.key.get(i));
				leftnode.tuplesPtr.add(node.tuplesPtr.get(i));

			}

			if (node.rightpointer != null)
				rightnode.rightpointer = node.rightpointer;
			else
				rightnode.rightpointer = null;
			if (node.leftpointer != null)
				leftnode.leftpointer = node.leftpointer;
			else
				leftnode.leftpointer = null;

			leftnode.rightpointer = rightnode;
			rightnode.leftpointer = leftnode;

			if (node.parent == null) {
				tempparent.isLeaf = false;
				tempparent.key.add(rightnode.key.get(0));
				tempparent.ptr.add(leftnode);
				tempparent.ptr.add(rightnode);
				leftnode.parent = tempparent;
				rightnode.parent = tempparent;
				root = tempparent;
				node = tempparent;

			} else if (node.parent != null) {
				// problem here that the root is not serialized
				parent = (node.parent);
				parent.key.add(rightnode.key.get(0));
				parent.key = sort(parent.key);
				leftnode.parent = parent;
				rightnode.parent = parent;
				newPosKey = parent.key.indexOf(rightnode.key.get(0));

				if (newPosKey < parent.key.size() - 1) {
					parent.ptr.add(null);

					for (int i = parent.key.size() - 1; i > newPosKey; i--) {
						parent.ptr.set(i + 1, parent.ptr.get(i));
					}

					parent.ptr.set(newPosKey + 1, rightnode);
					parent.ptr.set(newPosKey, leftnode);
				}

				else if (newPosKey == parent.key.size() - 1) {
					parent.ptr.set(newPosKey, leftnode);
					parent.ptr.add(rightnode);
				}
				if (node.leftpointer != null) {
					node.leftpointer.rightpointer = leftnode;
					leftnode.leftpointer = node.leftpointer;

				}
				if (node.rightpointer != null) {
					node.rightpointer.leftpointer = rightnode;
					rightnode.rightpointer = node.rightpointer;
				}
				if (parent.key.size() == Nodesize) {
					split(parent, Tablename, Columnname);
					return;
				} else {
					return;
				}
			}
		} else if (!node.isLeaf) {
			rightnode.isLeaf = false;
			if (node.key.size() % 2 == 0)
				split = (node.key.size() / 2) - 1;
			else
				split = node.key.size() / 2;

			Object popKey = (Object) node.key.get(split);
			int k = 0, p = 0;
			for (int i = split + 1; i < node.key.size(); i++) {
				rightnode.key.add(node.key.get(i));
			}
			for (int i = split + 1; i < node.ptr.size(); i++) {
				rightnode.ptr.add(node.ptr.get(i));
				rightnode.ptr.get(k++).parent = rightnode;

			}
			k = 0;
			for (int i = 0; i < split; i++) {
				leftnode.key.add(node.key.get(i));
			}

			for (int i = 0; i < split + 1; i++) {
				leftnode.ptr.add(node.ptr.get(i));
				leftnode.ptr.get(p++).parent = leftnode;
			}
			p = 0;
			if (node.parent == null) {
				tempparent.isLeaf = false;
				tempparent.key.add(popKey);
				tempparent.ptr.add(leftnode);
				tempparent.ptr.add(rightnode);
				leftnode.parent = tempparent;
				rightnode.parent = tempparent;
				node = tempparent;
				root = tempparent;
				return;
			} else if (node.parent != null) {
				parent = node.parent;
				parent.key.add(popKey);
				parent.key = sort(parent.key);
				newPosKey = parent.key.indexOf(popKey);
				if (newPosKey == parent.key.size() - 1) {
					parent.ptr.set(newPosKey, leftnode);
					parent.ptr.add(rightnode);
					rightnode.parent = parent;
					leftnode.parent = parent;
				} else if (newPosKey < parent.key.size() - 1) {
					int ptrSize = parent.ptr.size();
					parent.ptr.add(null);
					for (int i = ptrSize - 1; i > newPosKey; i--) {
						parent.ptr.set(i + 1, parent.ptr.get(i));
					}

					parent.ptr.set(newPosKey, leftnode);
					parent.ptr.set(newPosKey + 1, rightnode);
					leftnode.parent = parent;
					rightnode.parent = parent;
				}

				if (parent.key.size() == Nodesize) {
					split(parent, Tablename, Columnname);
					return;
				} else {
					return;
				}
			}
		}

	}

	public int countOfTuples(Node root, Object key, String tableName) throws FileNotFoundException, DBAppException {
		Node node = search(tableName, root, key);
		int index = searchInNode(node, key);
		int x;
		if (index == -1)
			return 0;
		else
			x = node.tuplesPtr.get(index).size();
		node = null;
		return x;
	}

	public void deleteOneTuplePtr(Node root, Object key, String pageLocation, String tableName)
			throws FileNotFoundException, DBAppException {
		Node node = search(tableName, root, key);
		int index = searchInNode(node, key);
		node.tuplesPtr.get(index).remove(pageLocation);

	}

	public void delete(Node root, Object key) throws FileNotFoundException, DBAppException {
		if (root == null)
			return;

		int splitIndex = deleteHelper(key, root, null, -1);
		if (splitIndex != -1) {
			root.key.remove(splitIndex);
			if (root.key.isEmpty()) {
				root = root.ptr.get(0);
			}
		}

		Node node = search(root, key);
		int x = 0;
		if (node != null) {
			x = searchInNode(node, key);
			node.key.set(x, smallestInRightSub(node.ptr.get(x + 1)));
		}

		// if the new root is also empty, then the entire tree must be empty
		if (root.key.isEmpty()) {
			root = null;
		}
	}

	private int deleteHelper(Object key, Node child, Node parent, int splitIndex)
			throws FileNotFoundException, DBAppException {
		if (parent != null) {
			child.setParent(parent);
		}

		// If child is a leaf, delete the key value pair from it
		if (child.isLeaf) {
			Node leaf = child;
			for (int i = 0; i < leaf.key.size(); i++) {
				if (equals(key, (leaf.key.get(i)))) {
					leaf.key.remove(key);
					leaf.tuplesPtr.remove(i);
					break;
				}
			}

			// Handle leaf underflow
			if (leaf.isUnderflowed() && leaf != root) {
				if (leaf.getIndexInParent() == 0) {
					return handleLeafNodeUnderflow(leaf, leaf.rightpointer, (leaf.getParent()));
					// serialize nodes in the method
				} else {
					return handleLeafNodeUnderflow(leaf.leftpointer, leaf, (leaf.getParent()));
					// serialize nodes in the method
				}
			}

		} else {
			Node index = child;

			int x = 0;
			for (int i = 0; i < index.key.size(); i++) {
				if (compare(index.key.get(i), key)) {
					if (!equals(key, (index.key.get(i)))) {
						splitIndex = deleteHelper(key, index.ptr.get(i), index, splitIndex);
						break;
					}
				}
				x++;
			}
			if (x == index.key.size()) {
				splitIndex = deleteHelper(key, index.ptr.get(index.ptr.size() - 1), index, splitIndex);
			}
			index = null;
		}

		// delete split key and handle overflow
		if (splitIndex != -1 && child != root) {
			child.key.remove(splitIndex);
			if (child.isUnderflowed()) {
				if (child.getIndexInParent() == 0) {
					Node rightSibling = child.getParent().ptr.get(child.getIndexInParent() + 1);
					splitIndex = handleIndexNodeUnderflow(child, rightSibling, child.getParent());

				} else {
					Node leftSibling = child.getParent().ptr.get(child.getIndexInParent() - 1);
					splitIndex = handleIndexNodeUnderflow(leftSibling, child, child.getParent());

				}
			} else
				splitIndex = -1;
		}
		return splitIndex;
	}
	// serialize nodes in the methods

	public Object smallestInRightSub(Node node) throws FileNotFoundException, DBAppException {
		if (node.isLeaf)
			return node.key.get(0);
		else

			return smallestInRightSub(node.ptr.get(0));

	}

	public int handleLeafNodeUnderflow(Node left, Node right, Node parent)
			throws FileNotFoundException, DBAppException {

		// If redistributable
		int totalSize = left.key.size() + right.key.size();
		if (totalSize >= 2 * left.minKeysLeaf) {

			int childIndex = parent.ptr.indexOf(right);

			// Store all key and values from left to right
			ArrayList<Object> key = new ArrayList<Object>();
			ArrayList<ArrayList<String>> vals = new ArrayList<ArrayList<String>>();
			key.addAll(left.key);
			key.addAll(right.key);
			vals.addAll(left.tuplesPtr);
			vals.addAll(right.tuplesPtr);

			int leftSize = totalSize / 2;

			left.key.clear();
			right.key.clear();
			left.tuplesPtr.clear();
			right.tuplesPtr.clear();

			// Add first half key and values into left and rest into right
			left.key.addAll(key.subList(0, leftSize));
			left.tuplesPtr.addAll(vals.subList(0, leftSize));
			right.key.addAll(key.subList(leftSize, key.size()));
			right.tuplesPtr.addAll(vals.subList(leftSize, vals.size()));

			parent.key.set(childIndex - 1, parent.ptr.get(childIndex).key.get(0));

			return -1;
		} else {
			// remove right child
			left.key.addAll(right.key);
			left.tuplesPtr.addAll(right.tuplesPtr);

			left.rightpointer = right.rightpointer;
			if (right.rightpointer != null) {
				right.rightpointer.leftpointer = left;

			}
			int index = parent.ptr.indexOf(right) - 1;
			parent.ptr.remove(right);

			return index;
		}
	}

	public int handleIndexNodeUnderflow(Node left, Node right, Node parent)
			throws FileNotFoundException, DBAppException {
		int splitIndex = -1;
		for (int i = 0; i < parent.key.size(); i++) {
			if (parent.ptr.get(i) == left && parent.ptr.get(i + 1) == right) {
				splitIndex = i;
				break;
			}
		}

		// Redistribute if possible
		if ((left.key.size() + right.key.size()) >= (2 * left.minKeysNonLeaf)) {

			// All key including the parent key
			ArrayList<Object> key = new ArrayList<Object>();
			ArrayList<Node> ptr = new ArrayList<Node>();
			key.addAll(left.key);
			key.add(parent.key.get(splitIndex));
			key.addAll(right.key);
		//	ArrayList<String> ptrString = new ArrayList<String>();

			ptr.addAll(left.ptr);
			ptr.addAll(right.ptr);

			// Get the index of the new parent key
			int newIndex = key.size() / 2;
			if (key.size() % 2 == 0) {
				newIndex -= 1;
			}
			parent.key.set(splitIndex, key.get(newIndex));

			left.key.clear();
			right.key.clear();
			left.ptr.clear();
			right.ptr.clear();

			left.key.addAll(key.subList(0, newIndex));
			right.key.addAll(key.subList(newIndex + 1, key.size()));
			left.ptr.addAll(ptr.subList(0, newIndex + 1));
			right.ptr.addAll(ptr.subList(newIndex + 1, ptr.size()));
		
			return -1;
		} else {
			left.key.add(parent.key.get(splitIndex));
			left.key.addAll(right.key);
			left.ptr.addAll(right.ptr);
			parent.ptr.remove(parent.ptr.indexOf(right));
			return splitIndex;
		}
	}

	public static boolean compare(Object x, Object y) {
		DBApp dbApp = new DBApp();
		boolean f = false;

		if (x instanceof Integer) {
			if ((Integer) x >= (Integer) y)
				f = true;
			else
				f = false;
		}

		if (x instanceof Polygon) {

			switch (dbApp.polygonCompare) {
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
				if (dbApp.polygonSelect) {
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
				if (dbApp.updatePolygon) {
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


	public double areaPolygon(Polygon polygon) {
		return  (((Polygon) polygon).getBounds().getSize().width) * (((Polygon) polygon).getBounds().getSize().height);
	}
	
	public void toStringto(Node root) throws FileNotFoundException, DBAppException {
		if (root == null)
			return;
		for (int i = 0; i < root.key.size(); i++) {
			System.out.print(areaPolygon((Polygon)root.key.get(i)) + " ");
		}
		count++;
		System.out.println(" ");
		for (int i = 0; i < root.ptr.size(); i++) {
			toStringto(root.ptr.get(i));
		}
	}

	public void toStringtoLeaf() throws FileNotFoundException, DBAppException {
		toStringtoLeaf(root);
	}

	public void toStringtoLeaf(Node root) throws FileNotFoundException, DBAppException {
		if (root == null)
			return;
		if (root.isLeaf) {
			System.out.println(root.key);
		} else {

			for (int i = 0; i < root.ptr.size(); i++) {
				toStringtoLeaf((root.ptr.get(i)));
			}
		}
	}

	public void toStringto() throws FileNotFoundException, DBAppException {
		toStringto(root);
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

	public static ArrayList<Object> sort(ArrayList<Object> array) {
		for (int i = 1; i < array.size(); i++) {
			Object value = (Object) array.get(i);
			int j = i - 1;
			while (j >= 0 && compare(array.get(j), value)) {
				// nums[j + 1] = nums[j];
				array.remove(j + 1);
				array.add(j + 1, array.get(j));
				// array.remove(j);
				j = j - 1;
			}
			// nums[j + 1] = value;
			array.remove(j + 1);
			array.add(j + 1, value);
		}
		return array;
	}

	public Node search(Node node, Object key) throws FileNotFoundException, DBAppException {
		if (node.isLeaf) {
			return null;
		} else {
			int x = searchInNode(node, key);
			if (x != -1)
				return node;

			for (int i = 0; i < node.key.size(); i++) {
				if (compare(node.key.get(i), key))
					return search((node.ptr.get(i)), key);

			}
			return search((node.ptr.get(node.ptr.size() - 1)), key);

		}
	}

	// takes root and returns the leaf node this object inside
	public Node search(String tableName, Node node, Object key) throws FileNotFoundException, DBAppException {
		if (node.isLeaf || node.key.size() == 0)
			return node;
		else {

			for (int i = 0; i < node.key.size(); i++) {
				if (!compare(key, node.key.get(i))) {
					return search(tableName, (node.ptr.get(i)), key);
				}
			}

			return search(tableName, (node.ptr.get(node.ptr.size() - 1)), key);

		}

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

	public static boolean equals(Object x, Object y) {
		if (x instanceof Polygon) {
			switch (DBApp.polygonCompare) {
			case "insert":
				int z = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
				int w = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
				if (z == w) {
					return true;
				} else
					return false;

			case "select":
				// polygonSelect = true? ----> =/!=
				if (DBApp.polygonSelect) {
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
				if (DBApp.updatePolygon) {
					int z1 = (((Polygon) x).getBounds().getSize().width) * (((Polygon) x).getBounds().getSize().height);
					int w1 = (((Polygon) y).getBounds().getSize().width) * (((Polygon) y).getBounds().getSize().height);
					if (z1 >= w1) {
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

	// takes root and returns all locations for this object
	public ArrayList<String> searchForLocation(String tableName, Node node, Object key)
			throws DBAppException, FileNotFoundException {
		Node leaf = search(tableName, node, key);
		int index = searchInNode(leaf, key);
		if (index < 0)
			throw new DBAppException("This record is not found");

		ArrayList<String> arrayList = leaf.tuplesPtr.get(index);
		leaf = null;
		node = null;
		return arrayList;
	}

	public ArrayList<String> searchForLocation1(String tableName, Node node, Object key)
			throws DBAppException, FileNotFoundException {
		Node leaf = search(tableName, node, key);
		int index = searchInNode(leaf, key);
		if (index < 0)
			return new ArrayList<String>();
		ArrayList<String> strings = leaf.tuplesPtr.get(index);
		leaf = null;
		node = null;
		return strings;
	}

	// returns index of object inside a node
	public int searchInNode(Node node, Object key) {
		return searchInNodeHelper(node.key, 0, node.key.size() - 1, key);
	}

	public int searchInNodeHelper(ArrayList<Object> array, int l, int h, Object key) {
		if (h >= l) {
			int mid = l + (h - l) / 2;
			// If the element is present at the middle itself
			if (equals(key, (array.get(mid)))) {
				return mid;

			}

			// If element is smaller than mid, then it can only be present in left subarray
			if (!compare(key, array.get(mid)))
				return searchInNodeHelper(array, l, mid - 1, key);
			// Else the element can only be present in right subarray
			return searchInNodeHelper(array, mid + 1, h, key);
		}
		// We reach here when element is not present in array
		return -1;
	}

	public void updateTuplePtr(Node root, String oldLocation, String newLocation, Object key, String tableName)
			throws FileNotFoundException, DBAppException {
		Node node = search(tableName, root, key);
		int index = 0;
		if (key instanceof Polygon) {
			for (int i = 0; i < node.key.size(); i++) {
				if (equals(key, node.key.get(i))) {
					index = i;
					break;
				}
			}
		} else
			index = searchInNode(node, key);

		for (int i = 0; i < node.tuplesPtr.get(index).size(); i++) {
			if (node.tuplesPtr.get(index).get(i).equals(oldLocation)) {
				node.tuplesPtr.get(index).set(i, newLocation);
				break;
			}
		}

		node = null;
		root = null;
	}

	public boolean isFound(Node root, Object key, String tableName) throws FileNotFoundException, DBAppException {
		Node leaf = search(tableName, root, key);
		int index = searchInNode(leaf, key);
		if (index == -1)
			return false;
		else
			return true;

	}

	public static void main(String[] args) throws IOException, DBAppException {
		Node root = new Node("", "");
		BPTreeIndex tree = new BPTreeIndex("Student");
		tree.root = root;

	}

}