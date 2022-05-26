import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.chrono.MinguoChronology;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.util.stream.Collectors;

public class DBApp implements DBAppInterface{
	

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		Table t = new Table(tableName,colNameType,colNameMin,colNameMax,clusteringKey);
		String path = "src/main/resources/data/tables/" + tableName + ".ser";
		Serializet(t,path);
	}
	
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException 
	{
		Table t = Deserializet("src/main/resources/data/tables/" + tableName + ".ser" );
		String metadata ="";
		try {
			t.createIndex(columnNames);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Serializet(t,"src/main/resources/data/tables/" + t.name + ".ser");
	}
	
	
	

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		Table t =Deserializet("src/main/resources/data/tables/" + tableName +".ser");
		try {
			t.insert(colNameValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!t.indeces.isEmpty()) {
			for(int i=0;i<t.indeces.size();i++) {
				Index x = Table.Deserializei(t.indeces.get(i));
				try {
					t.createIndex(x.colNames);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		String path = "src/main/resources/data/tables/" + tableName + ".ser";
		Serializet(t,path);
		
	}

	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		String path = "src/main/resources/data/tables/"+tableName+".ser";
		Table t = Deserializet(path);
		try {
			t.update(clusteringKeyValue,columnNameValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!t.indeces.isEmpty()) {
			for(int i=0;i<t.indeces.size();i++) {
				Index x = Table.Deserializei(t.indeces.get(i));
				try {
					t.createIndex(x.colNames);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		Serializet(t,path);
		
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		String path = "src/main/resources/data/tables/"+tableName+".ser";
		Table t = Deserializet(path);
		SQLTerm[] sql = hashtableToSql(tableName,columnNameValue);
		String[] operators= new String[sql.length-1];
		for(int i=0;i<operators.length;i++) {
			operators[i]="AND";
		}
		try {
			t.deletenew(sql,operators);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!t.indeces.isEmpty()) {
			for(int i=0;i<t.indeces.size();i++) {
				Index x = Table.Deserializei(t.indeces.get(i));
				try {
					t.createIndex(x.colNames);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}		
		Serializet(t,path);
		
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		String tableName = sqlTerms[0]._strTableName;
		String path = "src/main/resources/data/tables/"+tableName+".ser";
		Table t = Deserializet(path);
		Iterator i = null;
		try {
			i= t.select(sqlTerms,arrayOperators);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Serializet(t, path);
		return i;
	}
	public static void Serializet(Table t, String path) {
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream(path);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(t);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
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
	         System.out.println("Page not found");
	         c.printStackTrace();
	      }
		return t;
		
	}
	public static SQLTerm[] hashtableToSql(String tableName,Hashtable<String,Object> htbl){
        ArrayList<String> keys = Collections.list(htbl.keys());
        SQLTerm[] sqlTerms = new SQLTerm[keys.size()];
        for(int i=0;i<keys.size();i++){
            sqlTerms[i] = new SQLTerm(tableName,keys.get(i),"=",htbl.get(keys.get(i)));
        }
        return sqlTerms;
    }
}
