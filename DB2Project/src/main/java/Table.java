import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;



@SuppressWarnings("unchecked")
public class Table implements Serializable   {
	public String name;
	private Hashtable<String,String> ColType;
	private Hashtable<String,String> Min;
	private Hashtable<String,String> Max;
	private String ClusteringKey;
	private ArrayList pages;
	public ArrayList<String> indeces;
	private ArrayList<ArrayList<String>>indexedColumns;
	public Table() {
		
	}
	public Table(String name, Hashtable<String, String> colType, Hashtable<String, String> min,
			Hashtable<String, String> max, String clusteringKey) {
		
		this.name = name;
		ColType = colType;
		Min = min;
		Max = max;
		ClusteringKey = clusteringKey;
		pages = new ArrayList();
		try {
			FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv",true);
			Enumeration<String> e = colType.keys();
			while(e.hasMoreElements()) 
			{
				String clust = "False";
				String key = e.nextElement();
				if(key.equals(ClusteringKey)) clust = "True"; 
				csvWriter.append(name + ",");
				csvWriter.append(key + ",");
				csvWriter.append(colType.get(key) + ",");
				csvWriter.append(clust + ",");
				csvWriter.append("False," + min.get(key).toString() + ",");
				csvWriter.append(max.get(key) + "\n");
			}
			csvWriter.flush();
			csvWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		indeces = new ArrayList<String>();
		indexedColumns = new ArrayList<ArrayList<String>>();
		
	}

	
	
	@SuppressWarnings("unchecked")
	public void insert(Hashtable<String,Object> values) throws IOException, DBAppException {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
		    is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
		    ex.printStackTrace();
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		    ex.printStackTrace();
		}
		int maxInPage =  Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		if(values.isEmpty()) throw new DBAppException();
		if(!values.containsKey(ClusteringKey))throw new DBAppException();
		Hashtable<String,String> columns = new Hashtable<String, String>();
		Hashtable<String,String> minim = new Hashtable<String,String>();
		Hashtable<String,String> maxim = new Hashtable<String,String>();
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String row;
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    if(data[0].equals(name)) {
		    	columns.put(data[1],data[2]);
		    	minim.put(data[1], data[5]);
		    	maxim.put(data[1], data[6]);
		    }
		}
		csvReader.close();
		Set<String> keys= values.keySet();
		for(String key:keys) {
			if(!columns.containsKey(key)) { 
				throw new DBAppException();
			
			}
			switch (columns.get(key))
			{
			case "java.lang.Integer": 
				if (!((Integer)values.get(key) instanceof Integer) 
					|| (Integer)values.get(key)<(Integer.valueOf(this.Min.get(key)))
					|| (Integer)values.get(key)>(Integer.valueOf(this.Max.get(key))))
					throw new DBAppException();
				break;
			case "java.lang.String": 
				if (!((String)values.get(key) instanceof String)){
						throw new DBAppException();
				}	
				else if(((String)values.get(key)).compareTo(minim.get(key)) <0) {
					throw new DBAppException();
				}
				else if((((String)values.get(key)).compareTo(maxim.get(key)))>0) {
					throw new DBAppException();
				}
				break;
			case "java.lang.Double": 
				if (!((Double)values.get(key) instanceof Double)
					||(Double)values.get(key)<(Double.valueOf(this.Min.get(key)))
					||(Double)values.get(key)>(Double.valueOf(this.Max.get(key))))
					throw new DBAppException();
				break;
			default:
				try {
					if (!(values.get(key) instanceof Date)
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Min.get(key))))<0
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Max.get(key))))>0)
						throw new DBAppException();
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
				
			}
		}
		//done with metadata check on inserted values
				
		
		
		//if the arraylist of pages is empty, then there are no records inserted yet in the table,
		//socreate a new vector, insert the hasthable in it, and add the clustkey of this record in the 1st position 
		//of the arraylist
		if(this.pages.isEmpty()) {
			@SuppressWarnings("rawtypes")
			Vector<Hashtable<String,Object>> v = new Vector<>();
			v.addElement(values);
			pages.add(0, (values.get(ClusteringKey)));
			Serialize(v,this.name,0);
		}
		//if the rraylist is not empty, execute the following
		else {
			int i = searchPage(pages, values.get(ClusteringKey)); //index in arraylist,page number 
			Vector<Hashtable<String,Object>> v = Deserialize("src/main/resources/data/pages/"+ this.name + i + ".ser" ); // deserialize the page
				int j = searchIn(v,values.get(ClusteringKey),ClusteringKey); //search in the page for the index to insert in
				if(j<v.size()) {
					if(v.get(j).get(ClusteringKey).equals(values.get(ClusteringKey))) throw new DBAppException();
					}
				if(j==0) {
					pages.remove(i);
					pages.add(i,values.get(ClusteringKey));
				}
				v.add(j, values);	
				//if the size of the vector hasn't exceeded 200, serialize the vector 
				if(v.size()<=maxInPage)Serialize(v,this.name,i);
				//if it has exceeded 200 execute the following
				//extract the record at index 200,  and serialize the vector
				else {
					Hashtable<String, Object> extra=(Hashtable<String, Object>) v.remove(maxInPage);
					Serialize(v,this.name,i);
					// if the above vector exists in the last page in the arraylist, create a new page
					// insert the extra record in it, and then serialize it
					if(i==pages.size()-1) {
						Vector<Hashtable<String,Object>> newv = new Vector<Hashtable<String,Object>>();
						newv.addElement(extra);
						Serialize(newv,this.name,pages.size());
						pages.add( extra.get(ClusteringKey));
					}
					// if it's not the last page, then we're gonna use the recursive call, but before,
					//we have to force this record to being inserted in the next page instead of the old one,
					//because it's bigger than the lasgt record in i but it's smaller than the 1st record in i++
					//so we update the arraylist with its value as the min value of the next page to force
					//it into inserting in the new page
					else {
						
						Vector v11=new Vector();
						
						while(!pages.isEmpty())
				        {

				            v11.addElement(pages.remove(0));

				        }
				        v11.remove(i+1);
						v11.add(i+1,extra.get(ClusteringKey));
				        
				        for(int c=0;c<v11.size();c++)
				        {
				            pages.add(v11.get(c));
				        }
				        this.insert(extra);
				        //recursive call
					}
				}
			
		}
		
	}
	

	public void deleteFromTable(Hashtable<String, Object> columnNameValue) throws DBAppException, IOException {

		//first i need to check how many pages in this table
		Hashtable<String,Object>values=columnNameValue;
		File dir = new File("src/main/resources/data/pages/");
		int noOfPages=0;
		  File[] allPages = dir.listFiles();
		  if (allPages != null) {
		    for (int i=0;i<allPages.length;i++) {  
		      String pageName = allPages[i].getName();
		      if(pageName.length()>=this.name.length()) {
		      String tableName = pageName.substring(0, this.name.length());
		      if(tableName.equals(this.name)) {
		    	  	noOfPages++;
		    	  }
		      }
		    }
		      
		    }
		  //validate the keys to delete on
		  
		  	
		  Hashtable<String,String> columns = new Hashtable<String, String>();
			Hashtable<String,String> minim = new Hashtable<String,String>();
			Hashtable<String,String> maxim = new Hashtable<String,String>();
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String row;
			while ((row = csvReader.readLine()) != null) {
			    String[] data = row.split(",");
			    if(data[0].equals(name)) {
			    	columns.put(data[1],data[2]);
			    	minim.put(data[1], data[5]);
			    	maxim.put(data[1], data[6]);
			    }
			}
			csvReader.close();
			Set<String> keyss= values.keySet();
			for(String key:keyss) {
				if(!columns.containsKey(key)) { 
					throw new DBAppException();
				
				}
				switch (columns.get(key))
				{
				case "java.lang.Integer": 
					if (!((Integer)values.get(key) instanceof Integer) 
						|| (Integer)values.get(key)<(Integer.valueOf(this.Min.get(key)))
						|| (Integer)values.get(key)>(Integer.valueOf(this.Max.get(key))))
						throw new DBAppException();
					break;
				case "java.lang.String": 
					if (!((String)values.get(key) instanceof String)){
							throw new DBAppException();
					}	
					else if(((String)values.get(key)).compareTo(minim.get(key)) <0) {
						throw new DBAppException();
					}
					else if((((String)values.get(key)).compareTo(maxim.get(key)))>0) {
						throw new DBAppException();
					}
					break;
				case "java.lang.Double": 
					if (!((Double)values.get(key) instanceof Double)
						||(Double)values.get(key)<(Double.valueOf(this.Min.get(key)))
						||(Double)values.get(key)>(Double.valueOf(this.Max.get(key))))
						throw new DBAppException();
					break;
				default:
					try {
						if (!(values.get(key) instanceof Date)
							||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Min.get(key))))<0
							||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Max.get(key))))>0)
							throw new DBAppException();
					} catch (ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					break;
					
				}
			}
			
		//then i need to loop over those pages, load each one, check whether the row meets the conditions or not and if it does delete 
		  
		//looping over the pages
		for(int i=0;i<noOfPages;i++) {
			//first load the page vector
			Vector<Hashtable<String,Object>> page = new Vector<Hashtable<String,Object>>();
			String filename = "src/main/resources/data/pages/" + this.name + i + ".ser";
			
			try
	        {   
	            // Reading the object from a file
	            FileInputStream file = new FileInputStream(filename);
	            ObjectInputStream in = new ObjectInputStream(file);
	              
	            // Method for deserialization of object
	            page = (Vector<Hashtable<String, Object>>)in.readObject();
	              
	            in.close();
	            file.close();
	        } catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			
			//loop over the hashtable keys
			ArrayList<String> keys = Collections.list(columnNameValue.keys());
			
			//loop over the elements
			for(int j=0;j<page.size();j++) {
				boolean delete = true;
				for(int k=0;k<keys.size();k++) {
					Object inPage = page.get(j).get(keys.get(k)).toString();
					Object inDelete = columnNameValue.get(keys.get(k)).toString();
					if(!inPage.equals(inDelete)) {
						delete=false;
					}
				}
				if(delete) {
					page.remove(j--);
				}
			}
			
			
			//checking if this was the last row and the page should be deleted
			boolean empty= page.isEmpty();
			noOfPages--;
			if(empty) {
			
			//delete the page if it is empty
			
			File pageToDelete = new File(filename);
			pageToDelete.delete();
			
			if(noOfPages==0)break;
			
			//shift pages up if you deleted
			 dir = new File("src/main/resources/data/pages/");
			 allPages = dir.listFiles();
			  if (allPages != null) {
			    for (int l=0;i<allPages.length;l++) {
			      String oldName = allPages[l].getName();
			      String tableName = oldName.substring(0, this.name.length());
			      String newName = "";
			      boolean fromTable = tableName.equals(this.name);
			      if(fromTable) {
			    	  int index = Integer.parseInt(oldName.substring(tableName.length()-1, oldName.length()-3));
			    	  if(index>i) {
			    		  index--;
			    		  newName = tableName + index + ".ser";
			    		  allPages[i].renameTo(new File(newName));
			    	  }
			      }
			      
			    
			
			    }
			  }
			  
			  
			}
			else {
				//save the page
			
				FileOutputStream file = new FileOutputStream(filename);
	            ObjectOutputStream out = new ObjectOutputStream(file);
	   
	            out.writeObject(page);
	              
	            out.close();
	            file.close();
			
			//update the min value in the arraylist above
			
				Hashtable<String,Object> firstRow = page.get(0);
				Object newMin = firstRow.get(ClusteringKey);
				pages.set(0, newMin);
		}
		}
		
		
		

	}
	public static int searchPage(ArrayList a,Object key)
    {
         String keyString=key.toString();
         ArrayList <String> stringKeys=new ArrayList();
for (int i=0;i<a.size();i++)
{
    stringKeys.add(a.get(i).toString());
}
if (stringKeys.contains(keyString)) return stringKeys.indexOf(keyString);

        int left = 0;
        int right = a.size() - 1;
        int returned = -1;
        if (keyString.compareTo(stringKeys.get(0))<0)
            returned = 0;
        if (keyString.compareTo(stringKeys.get(right))>0)
            returned = right;


        ;

        while (left <= right) {
            if (returned != -1) return returned;
            int mid = left + ((right - left) / 2);
            if ( stringKeys.get(mid).compareTo(keyString)<0 &&  stringKeys.get(mid + 1).compareTo(keyString)>0)
                return mid;
            if ( stringKeys.get(mid - 1).compareTo(keyString)<0 &&  stringKeys.get(mid).compareTo(keyString)>0 && left != right) {
                return mid - 1;
            } else if (keyString.compareTo(stringKeys.get(mid))<0) right = mid - 1;
            else left = mid + 1;

        }
        return returned;

    }
	
	public static int searchIn(Vector<Hashtable<String,Object>> a,Object key,String ClusteringKey)
    {
        if(a.isEmpty())return 0;

      //  Boolean numInString=false;
        String keyString =key.toString();
Vector<String> stringOFkeys=new Vector();
for(int i=0;i<a.size();i++)
{
    stringOFkeys.add(a.get(i).get(ClusteringKey).toString());
}
if(stringOFkeys.contains(keyString)) return stringOFkeys.indexOf(keyString);





        int left=0;
        int right=a.size()-1;
        int returned=-1;
        if (keyString.compareTo(stringOFkeys.get(0))<0) {
            returned = 0;
            return returned;
        }

        if(keyString.compareTo(stringOFkeys.get(right))>0) {
            returned = right + 1;
            return returned;
        }



        while(left<=right)
        {
            //if(returned!=-1) return returned;
            int mid=left+((right-left)/2);

            if(stringOFkeys.get(mid).compareTo(keyString)<0 && stringOFkeys.get(mid+1).compareTo(keyString)>0)
            {
                return mid+1;
            }
            if(stringOFkeys.get(mid-1).compareTo(keyString)<0 && stringOFkeys.get(mid).compareTo(keyString)>0)
                return mid;

            else if(keyString.compareTo(stringOFkeys.get(mid))<0) right=mid-1;
            else left=mid+1;

        }

        return returned;




    }
	

	
	@SuppressWarnings("rawtypes")
	public static Vector<Hashtable<String,Object>> Deserialize(String path) {
		Vector<Hashtable<String,Object>> v=new Vector<Hashtable<String,Object>>();
		try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         v = (Vector<Hashtable<String,Object>>) in.readObject();
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
	public static void Serialize(Vector<Hashtable<String,Object>> v, String table,int index) {
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/main/resources/data/pages/"+ table + index + ".ser");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(v);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	public void update(String strClusteringKeyValue,Hashtable<String,Object> values) throws IOException, DBAppException{
		Hashtable<String,String> columns = new Hashtable<String, String>();
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String row;
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    if(data[0].equals(name)) {
		   // 	System.out.println(data[0]);
		    	columns.put(data[1],data[2]);    
		    }
		}
		csvReader.close();
		Enumeration<String> e= values.keys();
		outer:
		while(e.hasMoreElements()) {
			String key = e.nextElement();
			if(!columns.containsKey(key)) { 

				throw new DBAppException();
			}
			switch (columns.get(key))
			{
			case "java.lang.Integer": 
				if (!((Integer)values.get(key) instanceof Integer) 
					|| (Integer)values.get(key)<(Integer.valueOf(this.Min.get(key)))
					|| (Integer)values.get(key)>(Integer.valueOf(this.Max.get(key))))
					throw new DBAppException();
				break;
			case "java.lang.String": 
				if (!((String)values.get(key) instanceof String)){
						throw new DBAppException();
				}	
				else if(((String)values.get(key)).compareTo(this.Min.get(key)) <0) {
					throw new DBAppException();
				}
				else if((((String)values.get(key)).compareTo(this.Max.get(key)))>0) {
					throw new DBAppException();
				}
				break;
			case "java.lang.Double": 
				if (!((Double)values.get(key) instanceof Double)
					||(Double)values.get(key)<(Double.valueOf(this.Min.get(key)))
					||(Double)values.get(key)>(Double.valueOf(this.Max.get(key))))
					throw new DBAppException();
				break;
			default:
				try {
					if (!(values.get(key) instanceof Date)
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Min.get(key))))<0
						||(((Date)values.get(key)).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(this.Max.get(key))))>0)
						throw new DBAppException();
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
				
			}
		}
		
       int i = searchPage(pages,strClusteringKeyValue);
		Vector v = Deserialize("src/main/resources/data/pages/"+ this.name + i + ".ser");
		int j = searchIn(v,strClusteringKeyValue,ClusteringKey);
		Hashtable<String,Object> record = (Hashtable<String,Object>)v.get(j);
		if(record.get(ClusteringKey).equals(strClusteringKeyValue)) {
			Enumeration<String> en= values.keys();
			while(en.hasMoreElements()) {
				String k = en.nextElement();
				if(record.containsKey(k))
					record.replace(k, values.get(k));
				else
					record.put(k, values.get(k));
			}
		}
		v.remove(j);
		v.insertElementAt(record, j);
	
	}
	public void createIndex(String[] colNames) throws IOException, DBAppException {
		for(int i=0;i<colNames.length;i++) {
			boolean flag = false;
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String row;
			loop :
			while ((row = csvReader.readLine()) != null) {
			    String[] data = row.split(",");
			    if(data[0].equals(name)) {
			    	if(data[1].equals(colNames[i])) { flag = true; break loop;}
			    }
			}
			csvReader.close();
			if(!flag) throw new DBAppException();			
		}
		String[] min = new String[colNames.length];
		String[] max = new String[colNames.length];
		String[] coltype = new String[colNames.length];
		for(int i=0;i<colNames.length;i++) {
			min[i]=Min.get(colNames[i]);
			max[i]=Max.get(colNames[i]);
			coltype[i]=ColType.get(colNames[i]);
		}
		Index x = new Index(name,colNames,min,max,coltype);
		
		for(int i=0;i<colNames.length;i++) {
			String metadata= "";
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String row;
			loop :
			while ((row = csvReader.readLine()) != null) {
				boolean flag = false;
			    String[] data = row.split(",");
			    if(data[0].equals(this.name)) {
			    	if(data[1].equals(colNames[i])) { 
			    		data[4]="True";
			    		flag =true;
			    		for(int j=0;j<data.length;j++) {
			    			metadata+=data[j] + ",";
			    			if(j==data.length-1) {
			    				metadata+="\n";
			    			}
			    		}
			    	}
			    	else {
			    		metadata+= row + "\n";
			    	}
			    }
			    else {
			    	metadata+= row +"\n";
			    }
			}
			csvReader.close();
			try {
				File f= new File("src/main/resources/metadata.csv");
				f.delete();
				FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv",false);
				csvWriter.append(metadata);
				csvWriter.flush();
				csvWriter.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
		String index = "src/main/resources/data/Index/" + name;
		for(int i=0;i<colNames.length;i++) {
			index+= colNames[i];
		}
		index+=".ser";
		Serializei(x,index);
		indeces.add(index);
		ArrayList<String> columns = new ArrayList<String>();
		for(int k=0;k<colNames.length;k++) {
			columns.add(colNames[k]);
		}
		indexedColumns.add(columns);
		

	}
	public Iterator select(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException, IOException {
		Vector[] outputs = new Vector[sqlTerms.length];
		for(int i=0;i<sqlTerms.length;i++) {
			String path="";
			boolean flag = false;
			loop:
			for(int j=0;j<indexedColumns.size();j++) {
				if(indexedColumns.get(j).contains(sqlTerms[i]._strColumnName)) {
					path = indeces.get(j);
					flag= true;
					break loop;
				}
			}
			if(flag) {
				Index x = Deserializei(path);
				Vector<int[]> v = x.select(sqlTerms[i]);
				outputs[i]=v;
			}
			else {
				Vector<int[]>thiscolumn= new Vector<int[]>();
				//first i need to check how many pages in this table
				File dir = new File("src/main/resources/data/pages/");
				int noOfPages=0;
				  File[] allPages = dir.listFiles();
				  if (allPages != null) {
				    for (int l=0;l<allPages.length;l++) {  
				      String pageName = allPages[l].getName();
				      if(pageName.length()>=this.name.length()) {
				      String tableName = pageName.substring(0, this.name.length());
				      if(tableName.equals(this.name)) {
				    	  	noOfPages++;
				    	  }
				      }
				    }
				      
				    }
				  //validate the keys to delete on
				//then i need to loop over those pages, load each one, check whether the row meets the conditions or not and if it does delete 
				  
					//looping over the pages
					for(int j=0;j<noOfPages;j++) {
						//first load the page vector
						Vector<Hashtable<String,Object>> page = new Vector<Hashtable<String,Object>>();
						String filename = "src/main/resources/data/pages/" + this.name + j + ".ser";
						
						try
				        {   
				            // Reading the object from a file
				            FileInputStream file = new FileInputStream(filename);
				            ObjectInputStream in = new ObjectInputStream(file);
				              
				            // Method for deserialization of object
				            page = (Vector<Hashtable<String, Object>>)in.readObject();
				              
				            in.close();
				            file.close();
				        } catch (ClassNotFoundException e1) {
							e1.printStackTrace();
						}
						switch(ColType.get(sqlTerms[i]._strColumnName)) {
							case "java.lang.Integer":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)>(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)>=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)<(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)<=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)==(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)!=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.Double":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)>(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)>=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)<(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)<=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)==(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)!=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.String":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)>0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)>=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)<0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)<=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((String)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if(!((String)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((String)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.Date":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)>0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)>=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)<0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)<=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((Date)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if(!((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((Date)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;	
						}
						
					}
					outputs[i]=thiscolumn;
			}
		}
		for(int i=0;i<arrayOperators.length;i++) {
			switch(arrayOperators[i]) {
				case "AND":
					outputs[i+1]=AND(outputs[i],outputs[i+1]); break;
				case "OR":
					outputs[i+1]=OR(outputs[i],outputs[i+1]); break;
				case "XOR":
					outputs[i+1]=XOR(outputs[i],outputs[i+1]); break;	
					
			}
		}
		Vector<int[]>finaal= outputs[outputs.length-1];
		Vector<Hashtable<String,Object>> records=new Vector<Hashtable<String,Object>>();
		for(int i=0;i<finaal.size();i++) {
			Vector<Hashtable<String,Object>> v =Deserialize("src/main/resources/data/pages/" + name + finaal.get(i)[0] + ".ser");
			records.add(v.get(finaal.get(i)[1]));
			Serialize(v,name,finaal.get(i)[0]);
		}
		
		
		return records.iterator();
	}
	public void deletenew(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException, IOException {
		Vector[] outputs = new Vector[sqlTerms.length];
		for(int i=0;i<sqlTerms.length;i++) {
			String path="";
			boolean flag = false;
			loop:
			for(int j=0;j<indexedColumns.size();j++) {
				if(indexedColumns.get(j).contains(sqlTerms[i]._strColumnName)) {
					path = indeces.get(j);
					flag= true;
					break loop;
				}
			}
			if(flag) {
				Index x = Deserializei(path);
				Vector<int[]> v = x.select(sqlTerms[i]);
				outputs[i]=v;
			}
			else {
				Vector<int[]>thiscolumn= new Vector<int[]>();
				//first i need to check how many pages in this table
				File dir = new File("src/main/resources/data/pages/");
				int noOfPages=0;
				  File[] allPages = dir.listFiles();
				  if (allPages != null) {
				    for (int l=0;l<allPages.length;l++) {  
				      String pageName = allPages[l].getName();
				      if(pageName.length()>=this.name.length()) {
				      String tableName = pageName.substring(0, this.name.length());
				      if(tableName.equals(this.name)) {
				    	  	noOfPages++;
				    	  }
				      }
				    }
				      
				    }
				  //validate the keys to delete on
				//then i need to loop over those pages, load each one, check whether the row meets the conditions or not and if it does delete 
				  
					//looping over the pages
					for(int j=0;j<noOfPages;j++) {
						//first load the page vector
						Vector<Hashtable<String,Object>> page = new Vector<Hashtable<String,Object>>();
						String filename = "src/main/resources/data/pages/" + this.name + j + ".ser";
						
						try
				        {   
				            // Reading the object from a file
				            FileInputStream file = new FileInputStream(filename);
				            ObjectInputStream in = new ObjectInputStream(file);
				              
				            // Method for deserialization of object
				            page = (Vector<Hashtable<String, Object>>)in.readObject();
				              
				            in.close();
				            file.close();
				        } catch (ClassNotFoundException e1) {
							e1.printStackTrace();
						}
						switch(ColType.get(sqlTerms[i]._strColumnName)) {
							case "java.lang.Integer":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)>(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)>=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)<(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)<=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)==(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if((Integer)page.get(k).get(sqlTerms[i]._strColumnName)!=(Integer)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.Double":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)>(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)>=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)<(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)<=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)==(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if((Double)page.get(k).get(sqlTerms[i]._strColumnName)!=(Double)sqlTerms[i]._objValue) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.String":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)>0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)>=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)<0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((String)sqlTerms[i]._objValue)<=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if(((String)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((String)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if(!((String)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((String)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;
							case "java.lang.Date":
								for(int k=0;k<page.size();k++) {
									switch(sqlTerms[i]._strOperator) {
										case ">":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)>0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case ">=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)>=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "<":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)<0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
										case "<=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).compareTo((Date)sqlTerms[i]._objValue)<=0) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "=":
											if(((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((Date)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;
										case "!=":
											if(!((Date)(page.get(k).get(sqlTerms[i]._strColumnName))).equals((Date)sqlTerms[i]._objValue)) {
												int[] thisrecord = {j,k};
												thiscolumn.add(thisrecord);
											} break;	
									}
								} break;	
						}
						
					}
					outputs[i]=thiscolumn;
			}
		}
		for(int i=0;i<arrayOperators.length;i++) {
			switch(arrayOperators[i]) {
				case "AND":
					outputs[i+1]=AND(outputs[i],outputs[i+1]); break;
				case "OR":
					outputs[i+1]=OR(outputs[i],outputs[i+1]); break;
				case "XOR":
					outputs[i+1]=XOR(outputs[i],outputs[i+1]); break;	
					
			}
		}
		Vector<int[]>finaal= outputs[outputs.length-1];
		for(int i=0;i<finaal.size();i++) {
			Vector<Hashtable<String,Object>> v =Deserialize("src/main/resources/data/pages/" + name + finaal.get(i)[0] + ".ser");
			v.remove(finaal.get(i)[1]);
			Serialize(v,name,finaal.get(i)[0]);
		}
		
		
		
	}
	public static void Serializei(Index i, String path) {
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream(path);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(i);
	         out.close();
	         fileOut.close();
	      } catch (IOException e) {
	         e.printStackTrace();
	      }
	}
	public static Index Deserializei(String path) {
		Index i=new Index();
		try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         i = (Index) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException e) {
	         e.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Page not found");
	         c.printStackTrace();
	      }
		return i;
		
	}
	public static Vector<int[]> XOR(Vector<int[]> s,Vector<int[]> v){
        for(int i=0;i<v.size();i++){
            for(int j=0;j<s.size();j++){
                boolean flag = false;
                if(Arrays.equals(v.get(i),s.get(j))) {
                    s.remove(j);
                    flag=true;
                }
                if(flag)
                    v.remove(i);
            }
        }
        for(int[] ints : s){
            v.add(ints);
        }
        return v;
    }

    public static Vector<int[]> OR(Vector<int[]> s,Vector<int[]> v){
        for(int i=0;i<v.size();i++){
            for(int j=0;j<s.size();j++){
                if(Arrays.equals(v.get(i),s.get(j))) {
                    s.remove(j);
                }
            }
        }
        for(int[] ints : s){
            v.add(ints);
        }
        return v;
    }

    public static Vector<int[]> AND(Vector<int[]> s,Vector<int[]> v){
        Vector<int[]> end = new Vector<int[]>();
        for(int i=0;i<v.size();i++){
            boolean valid = false;
            for(int j=0;j<s.size();j++){
                if(Arrays.equals(v.get(i),s.get(j))) {
                    valid = true;
                    break;
                }
            }
            if(valid)
                end.add(v.get(i));
        }
        return end;
    }
	public static void main(String[] args) throws IOException {
		File f= new File("src/main/resources/metadata.csv");
		f.delete();

	}
}

