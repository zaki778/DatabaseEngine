import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
	Vector tuples;
	String name;
	public Page(String name) {
		tuples = new Vector();
		this.name = name;		
	}
	public void Serialize() {
		 try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/main/resources/pages/"+name+".ser");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(tuples);
	         out.close();
	         fileOut.close();
	         System.out.printf("Serialized data is saved in + path");
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	public static void Deserialize(String path) {
		try {
	         FileInputStream fileIn = new FileInputStream("/tmp/employee.ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         Page p = (Page) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	         return;
	      } catch (ClassNotFoundException c) {
	         System.out.println("Employee class not found");
	         c.printStackTrace();
	         return;
	      }
		
	}

}
