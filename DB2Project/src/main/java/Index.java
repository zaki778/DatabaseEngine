import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class Index implements Serializable 
{
	private String tableName;
	public String[] colNames;
	private String[] min;
	private String[] max;
	private String[] colType;
	private int dimensions;
	private String[][] ranges;
	private Object[] GridIndex;
	private Vector[] divisions;
	private String[] buckets;
	public Index() {
		
	}
	public Index(String tableName, String[] colNames, String[] min, String[] max, String[] colType) throws DBAppException {
		this.tableName = tableName;
		this.colNames = colNames;
		this.min = min;
		this.max = max;
		this.colType = colType;
		this.dimensions = colNames.length;
		divisions = new Vector[dimensions];
		this.ranges= new String[dimensions][10];
		for(int i=0;i<colType.length;i++) {
			switch(colType[i]) {
			case "java.lang.Integer":
				ranges[i]= getRangesInt(min[i],max[i]); break;
			case "java.lang.Double":
				ranges[i]= getRangesDouble(min[i],max[i]); break;
			case "java.lang.String":
				ranges[i]= getRangesString(min[i],max[i]); break;
			default :
				ranges[i]= getRangesDate(min[i],max[i]); break;
			}
		}
		createBucketsFirst();
		
		
		
		
	}
	public static void main(String[] args) throws IOException {
//	Object[] array = MultiArray(2,2);
//	((Object[])array[0])[0]="omar";
//	((Object[])array[0])[1]="disha";
//	((Object[])array[1])[0]="ahmed";
//	((Object[])array[1])[1]="attawia";
//	for(int i=0;i<array.length;i++) {
//		for(int j=0;j<((Object[])array[0]).length;j++) {
//			System.out.println(((Object[])array[i])[j]);
//		}
//	}
//		String[]arr= {"1","2","3"};
//		String[]array= ArrayUtils.removeElement(arr, arr[0]);
		}
		

	
	public static String[] getRangesInt(String min,String max) {
		String[] v=new String[11];
		String[] r =new String[10];
		int minim = (Integer.valueOf(min));
		int maxim = (Integer.valueOf(max));
		int diff = maxim - minim;
		Double interval = (Double.valueOf(diff/10));
		for(int i=0;i<v.length;i++) {
			if(i==v.length-1) {
				v[i]="" + maxim;
			}
			else {
			v[i]="" + (minim + interval*i); 
			}
		}
		for(int i=0;i<r.length;i++) {
			r[i]=v[i] + " - " + v[i+1];
		}
		return r;
		
	}
	public static String[] getRangesString(String min,String max) {
		String[]v=splitTenRanges(min, max);
		String[]r=new String[10];
		for(int i=0;i<r.length;i++) {
			r[i]=v[i] + " - " + v[i+1];
		}
		return r;
	}
	public static String[] getRangesDouble(String min,String max) {
		String[] v = new String[11];
		String[] r = new String[10];
		Double minim = (Double.valueOf(min));
		Double maxim = (Double.valueOf(max));
		Double diff = maxim - minim;
		Double interval = diff/10.0;
		for(int i=0;i<v.length;i++) {
			if(i==v.length-1) {
				v[i]= "" + maxim;
			}
			else {
				v[i]= "" + (minim+ (interval*i));
			}
		}
		for(int i=0;i<r.length;i++) {
			r[i]=v[i] + " - " + v[i+1];
		}
		return r;
	}
	@SuppressWarnings("deprecation")
	public static String[] getRangesDate(String min,String max) {
		String[] v= new String[11];
		String[] r= new String[10];
		String[]minimum=min.split("-");
		String[]maximum=max.split("-");
		long minim = Date.UTC((Integer.valueOf(minimum[0]))-1900, (Integer.valueOf(minimum[1]))-1, (Integer.valueOf(minimum[2])),0,0,0);
		long maxim =  Date.UTC((Integer.valueOf(maximum[0]))-1900, (Integer.valueOf(maximum[1]))-1, (Integer.valueOf(maximum[2])),0,0,0);
		long diff = maxim - minim;
		long interval = diff/10;
		for(int i=0;i<v.length;i++) {
			long x = (minim + (interval*i));
			if(i==v.length-1) x=maxim;
			Date d = new Date(x);
			v[i]= d.toString();
		}
		for(int i=0;i<r.length;i++) {
			r[i]=v[i] + " - " + v[i+1];
		}
		return r;
		
		
	}
	public static Object[] MultiArray(int dimensions,int size) {
		if(dimensions<1)throw new IndexOutOfBoundsException();
		else if(dimensions==1) {
			Object[] arr = new Object[size];
			return arr;
		}
		else {
			Object[] array = new Object[size];
			for(int i=0;i<size;i++) {
				array[i]=MultiArray(dimensions-1,size);
			}
			return array;
		}				
	}
	public void createBucketsFirst() throws DBAppException {
		for(int i=0;i<(ranges[0].length);i++) {
			String[]bucketRanges= new String[dimensions];
			if(dimensions==1) {
				createBucketsFinal(bucketRanges,ranges[0],""); break;
			}
			else {
				String curr = "" + i;
				bucketRanges[0]=ranges[0][i];
				this.createBucketsNext(bucketRanges,curr,dimensions-1,1);	
			}			
		}
	}
	public void createBucketsNext(String[]bucketRanges, String curr,int currdimension,int rangesindex) throws DBAppException {
		if(currdimension==1) {
			createBucketsFinal(bucketRanges,ranges[ranges.length-1],curr);
		}
		else {
			for(int i=0;i<ranges[rangesindex].length;i++) {
				String s =curr + i;
				bucketRanges[rangesindex]=ranges[rangesindex][i];
				createBucketsNext(bucketRanges,s,currdimension-1,rangesindex+1);
			}
		}
	}
	public void createBucketsFinal(String[]bucketRanges,String[]finalarray,String curr) throws DBAppException {
		for(int i=0;i<finalarray.length;i++) {
			bucketRanges[bucketRanges.length-1]=finalarray[i];
			String bname= "Bucket" + tableName;
			for(int j=0;j<colNames.length;j++) {
				bname+=colNames[j];
			}
			bname+= curr + i;
			Bucket b = new Bucket(tableName,bname,colNames,colType,bucketRanges);
			Serializeb(b,"src/main/resources/data/buckets/" +bname + ".ser");
		}
	}
	public static void Serializeb(Bucket b , String path) {
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream(path);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(b);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	public static Bucket Deserializeb(String path) {
		Bucket b=new Bucket();
		try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         b = (Bucket) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Page not found");
	         c.printStackTrace();
	      }
		return b;
		
	}
	public static String encode(String str){
        //this method takes in a string and returns its equivalent ascii encoding in a string to avoid going over the long range
        //for example an input of "aaa" returns "097097097"
        //the zeros are for consistency to handle each 3 numbers as a character
        String ascii = "";
        for(int i=0;i<str.length();i++) {
            int num = (int)str.charAt(i);
            if(num/100>0)
                ascii+=num;
            else if(num/10>0)
                ascii+="0"+num;
            else
                ascii+="00"+num;
        }
        return ascii;
    }

    public static String decode(String ascii){
        //this method does the exact opposite of the encode method
        //returns the string equivalent of an ascii encoded string
        // "098098098" returns "bbb"
        String str="";
        for(int i=0;i<ascii.length();i+=3){
            int num = Integer.parseInt(ascii.substring(i,i+3));
            str += (char)num;
        }
        return str;
    }

    public static String[] splitTenRanges(String min,String max){
        //this method takes in two strings and returns an array for ranges between them, the array contains the minimum inclusive for each range
        //for example if the range will be split into 2, with the min being aa and the max being cc,
        //the ranges would be aa - bb and bb - cc where bb is included in the second range not the first one
        //the method would then return [aa,bb,cc] as these are the min values for each range, with the max being the value that follows it
        //and the last index is the absolute maximum

        //this part checks if the difference between the first two chars is 10 or more and if not goes to the next char
        String maxChar="";
        String minChar="";
        int maxCharEq=0;
        int minCharEq=0;
        String maxAscii = encode(max);
        String minAscii = encode(min);
        for(int i=3;i<maxAscii.length();i+=3){
            String maxTemp = maxAscii.substring(0,i);
            String minTemp = minAscii.substring(0,i);
            maxCharEq = Integer.parseInt(maxTemp);
            minCharEq = Integer.parseInt(minTemp);
            if(maxCharEq - minCharEq>9){
                maxChar = maxTemp;
                minChar = minTemp;
                break;
            }
        }

        //here we get the difference and fill an array with the ranges for each range (first range spans from 1-5 so difference of 4,
        // second range 5-8 so difference of 3 etc)
        int diff = (maxCharEq - minCharEq)/10;
        int[] range = new int[10];
        Arrays.fill(range,diff);
        int rem = (maxCharEq - minCharEq)%10;
        for(int i=0;i<rem;i++){
            range[i]++;
        }
        
        //here we add the min value for each range with the remaining characters as a for simplicity
        String[] ranges = new String[10];
        for(int i=0;i<ranges.length;i++){
            minCharEq+=range[i];
            String minCharTempStr = String.valueOf(minCharEq);
            if(minCharTempStr.length()%3 != 0)
                minCharTempStr = "0"+minCharTempStr;
            ranges[i] = (decode(minCharTempStr));
            while(ranges[i].length()<max.length()){
                ranges[i]+="a";
            }
        }
        ranges[9]=max;

        //this last bit rearranges the array and adds the max value in the last index
        for (int i = (9 - 1); i >= 0; i--) {
            ranges[i+1] = ranges[i];
        }

        ranges[0] = min;

        String[] arr = new String[11];
        for(int i=0;i< ranges.length;i++){
            arr[i]=ranges[i];
        }
        arr[10] = max;
        return arr;
    }
    public Vector select(SQLTerm sql) throws DBAppException {
    	Vector<int[]> allPositions = new Vector<int[]>();
    	Vector<String> v = new Vector<String>();
    	int colindex = 0; ;
    	int start=10;
    	for(int i=0;i<colNames.length;i++) {
    		if(colNames[i].equals(sql._strColumnName)) 
    			colindex=i;
    	}
    	switch(colType[colindex]) {
    		case "java.lang.Integer":
    			for(int i=0;i<ranges[colindex].length;i++) {
    				String[] splitted = ranges[colindex][i].split(" - ");
    				int value = (Integer)sql._objValue;
    				int min = Integer.valueOf(splitted[0]);
    				int max = Integer.valueOf(splitted[1]);
    				if(i==ranges[colindex].length-1) {
    					if(value>=min && value<=max) {
    						start=i;
    					}
    				}
    				else {
    				if(value>=min && value<max) start = i;
    				}
    			}
    			if(start==10)throw new DBAppException("Value out of Range");
    			break;
    		case "java.lang.Double":	
    			for(int i=0;i<ranges[colindex].length;i++) {
    				String[] splitted = ranges[colindex][i].split(" - ");
    				Double value = (Double)sql._objValue;
    				Double min = Double.valueOf(splitted[0]);
    				Double max = Double.valueOf(splitted[1]);
    				if(i==ranges[colindex].length-1) {
    					if(value>=min && value<=max) {
    						start=i;
    					}
    				}
    				else {
    				if(value>=min && value<max) start = i;
    				}
    			}
    			if(start==10)throw new DBAppException("Value out of Range");
    			break;
    		case "java.lang.String":
    			for(int i=0;i<ranges[colindex].length;i++) {
    				String[] splitted = ranges[colindex][i].split(" - ");
    				String value = (String)sql._objValue;
    				String min = splitted[0];
    				String max = splitted[1];
    				if(i==ranges[colindex].length-1) {
    					if(value.compareTo(min)>=0 && value.compareTo(max)<=0) {
    						start=i;
    					}
    				}
    				else {
    				if(value.compareTo(min)>=0 && value.compareTo(max)<0) start = i;
    				}
    			}
    			if(start==10)throw new DBAppException("Value out of Range");
    			break;
    		case "java.util.Date":
    			for(int i=0;i<ranges[colindex].length;i++) {
    				String[] splitted = ranges[colindex][i].split(" - ");
    				Date value = (Date)sql._objValue;
    				Date min = dateString(splitted[0]);
    				Date max = dateString(splitted[1]);
    				if(i==ranges[colindex].length-1) {
    					if(value.compareTo(min)>=0 && value.compareTo(max)<=0) {
    						start=i;
    					}
    				}
    				else {
    				if(value.compareTo(min)>=0 && value.compareTo(max)<0) start = i;
    				}
    			}
    			if(start==10)throw new DBAppException("Value out of Range");
    			break;			
    	}
    	switch(sql._strOperator) {
    	case ">": 
    		v = bucketsInSearch(dimensions,colindex,start,9); break;
    	case ">=":
    		v = bucketsInSearch(dimensions,colindex,start,9); break;
    	case "<":
    		v = bucketsInSearch(dimensions, colindex, 0, start); break;
    	case "<=":	
    		v = bucketsInSearch(dimensions, colindex, 0, start); break;
    	case "=":
    		v = bucketsInSearch(dimensions, colindex, start, start); break;
    	case "!=":
    		v = bucketsInSearchNE(dimensions);
    	}
    	String bucketname = "Bucket" + tableName;
    	for(int i=0;i<colNames.length;i++) {
    		bucketname+= colNames[i];
    	}
    	Object[] value= {sql._objValue};
    	String[] sqlCol = {sql._strColumnName};
    	switch(sql._strOperator) {
    	case ">":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketGreaterThan(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	case ">=":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketGreaterThanOrEqual(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	case "<":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketLessThan(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	case "<=":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketLessThanOrEqual(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	case "=":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketEquals(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	case "!=":
    		for(int i=0;i<v.size();i++) {
        		Vector<int[]>temp=new Vector<int[]>();
    			Bucket b =Deserializeb("src/main/resources/data/buckets/" + bucketname + v.get(i) + ".ser");
        		temp=b.selectFromBucketNotEquals(sqlCol, value);
        		while(!temp.isEmpty()) {
        			allPositions.add(temp.remove(0));
        		}
        	}
    	}
    	return allPositions;
    	
    	
    }
    public static Vector<String> bucketsInSearch(int dimension,int position,int minRange,int maxRange){
        int max = (int) Math.pow(10,dimension);
        String[] allValues = new String[max];
        for(int i=0;i<max;i++){
            String temp = ""+i;
            while(temp.length()<dimension){
                temp="0"+temp;
            }
            allValues[i]=temp;

        }

        Vector<String> toSearchIn = new Vector<String>();
        for(int i=0;i< allValues.length;i++){
            int curr = Integer.parseInt(allValues[i].substring(position,position+1));
            if(curr>=minRange && curr<=maxRange)
                toSearchIn.add(allValues[i]);
        }
        return toSearchIn;
    }
    public static Vector<String> bucketsInSearchNE(int dimension){
        int max = (int) Math.pow(10,dimension);
        Vector<String> allValues = new Vector<String>();
        for(int i=0;i<max;i++){
            String temp = ""+i;
            while(temp.length()<dimension){
                temp="0"+temp;
            }
            allValues.add(temp);

        }
        return allValues;
    }
    public static Date dateString(String dateStr){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String[] DateArr = dateStr.split(" ");
        String toMin = "";
        toMin+=DateArr[DateArr.length-1]+"-";
        switch(DateArr[1]){
            case "Jan":
                toMin+="01-";
                break;
            case "Feb":
                toMin+="02-";
                break;
            case "Mar":
                toMin+="03-";
                break;
            case "Apr":
                toMin+="04-";
                break;
            case "May":
                toMin+="05-";
                break;
            case "Jun":
                toMin+="06-";
                break;
            case "Jul":
                toMin+="07-";
                break;
            case "Aug":
                toMin+="08-";
                break;
            case "Sep":
                toMin+="09-";
                break;
            case "Oct":
                toMin+="10-";
                break;
            case "Nov":
                toMin+="11-";
                break;
            case "Dec":
                toMin+="12-";
                break;
        }
        toMin+=DateArr[2];
        Date minDate = null;
        try {
            minDate = format.parse(toMin);
        } catch (ParseException e) {}
        return minDate;
    }
	
	
 }
