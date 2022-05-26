import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class main implements Serializable {
	
	public static Vector Deserialize(String path) {
		Vector v=new Vector();
		try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         v = (Vector) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Page not found");
	         c.printStackTrace();
	      }
		return v;
		
	}
    public static int searchPage(ArrayList a,Object key)
    {
        ArrayList<Double> b=new ArrayList();
      for (int i=0;i<a.size();i++)
      {
          if(a.get(i) instanceof Double)
          {
              b.add((Double)a.get(i));
          }
          else if(a.get(i) instanceof Integer)
          {
              b.add(Double.valueOf((Integer)a.get(i)));
          }
      }
        if(a.contains(key))
            return a.indexOf(key);
        int temp;
        double convertedKey=0;
        if(key instanceof  Double)
            convertedKey=(Double)key;
        else if(key instanceof  Integer) {
            temp = (Integer) key;
            convertedKey=Double.valueOf(temp);
        }
        if(a.isEmpty())return 0;

        if (key instanceof Integer || key instanceof Double) {

            int left = 0;
            int right = a.size() - 1;
            int returned = -1;
            if (convertedKey <  b.get(0))
                returned = 0;
            if (convertedKey >  b.get(right))
                returned = right;


            ;

            while (left <= right) {
                if (returned != -1) return returned;
                int mid = left + ((right - left) / 2);
                if ( b.get(mid) < convertedKey &&  b.get(mid + 1) > convertedKey)
                    return mid;
                if ( b.get(mid - 1) < convertedKey &&  b.get(mid) > convertedKey && left != right) {
                    return mid - 1;
                } else if (convertedKey <  b.get(mid)) right = mid - 1;
                else left = mid + 1;

            }
            return returned;

        }else {
            String keyString=key.toString();

            int left=0;
            int right=a.size()-1;
            int returned=-1;

            if (keyString.compareTo(a.get(0).toString())<0)
                returned= 0;
            if(keyString.compareTo(a.get(right).toString())>0)
                returned= right+1;

            while(left<=right)
            {
                if(returned!=-1) return returned;
                int mid=left+((right-left)/2);
                if(a.get(mid).toString().compareTo(keyString)<0 && a.get(mid+1).toString().compareTo(keyString)>0 &&left!=right)
                    return mid+1;
                if(a.get(mid-1).toString().compareTo(keyString)<0 && a.get(mid).toString().compareTo(keyString)>0 &&left!=right)
                {
                    return mid;
                }

                else if(keyString.compareTo(a.get(mid).toString())<0) right=mid-1;
                else left=mid+1;

            }
            return returned;


        }
    }

	public static void main(String[] args) throws IOException, ParseException, DBAppException {
		Hashtable<String, Object> values = new Hashtable();
        values.put("gpa", 1.5);
        values.put("student_id", "34-9874");
        values.put("course_name", "bar");
      //  values.put("elective", true);


        @SuppressWarnings("deprecation")
		Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        values.put("date_passed", date_passed);

		if(!values.containsKey("student_id"))throw new DBAppException();
		Hashtable<String,String> columns = new Hashtable<String, String>();
		Hashtable<String,String> minim = new Hashtable<String,String>();
		Hashtable<String,String> maxim = new Hashtable<String,String>();
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String row;
		System.out.println("here");
		while ((row = csvReader.readLine()) != null) {
			System.out.println("here2");
		    String[] data = row.split(",");
		    if(data[0].equals("transcripts")) {
		    	columns.put(data[1],data[2]);
		    	minim.put(data[1], data[5]);
		    	maxim.put(data[1], data[6]);
		    }
		}
		csvReader.close();
		Enumeration<String> e= values.keys();
		outer:
		while(e.hasMoreElements()) {
			System.out.println("here3");
			String key = e.nextElement();
			System.out.println(key);
			if(!columns.containsKey(key)) throw new DBAppException();
			System.out.println(columns.get(key));
			switch (columns.get(key))
			{
			case "java.lang.Integer": 
				if (!(values.get(key) instanceof Integer) 
					|| (Integer)values.get(key)<(Integer.valueOf(minim.get(key)))
					|| (Integer)values.get(key)>(Integer.valueOf(maxim.get(key))))
					throw new DBAppException();
				break;
			case "java.lang.String": 
				if (!((String)values.get(key) instanceof String)){
						throw new DBAppException();
				}	
				else if(((String)values.get(key)).compareTo(minim.get(key)) <0) {
					System.out.println("here4");
					throw new DBAppException();
				}
				else if((((String)values.get(key)).compareTo(maxim.get(key)))>0) {
					throw new DBAppException();
				}
				break;
			case "java.lang.Double":
				System.out.println("heeeere");
				if (!(values.get(key) instanceof Double)
					||(Double)values.get(key)<(Double.valueOf(minim.get(key)))
					||(Double)values.get(key)>(Double.valueOf(maxim.get(key))))
					throw new DBAppException();
				System.out.println("safe");
				break;
			default:
				try {
					if (!(values.get(key) instanceof Date)
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(minim.get(key))))<0
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(maxim.get(key))))>0)
						throw new DBAppException();
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
				
			}
		}
//		Table t = new Table("Omar");
//		Serializet(t,"Omar");
//		Table v =Deserializet("src/main/resources/tables/Omar.ser");
//		System.out.println(v.name);
		
	}
	public static Table Deserializet(String path) {
		Table t=new Table();
		try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         t = (Table) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Table not found");
	         c.printStackTrace();
	      }
		return t;
		
	}
	public static void Serializet(Table t, String table) {
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/main/resources/tables/"+ table + ".ser");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(t);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}

}
