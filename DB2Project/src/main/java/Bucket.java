import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Bucket implements Serializable {
    Vector<Hashtable<String,Object>> references;
    String bucketName;
    Bucket overflow;
    String[] ranges;
    String[] colNames;
    String[] colType;
    Object[] minValue;
    Object[] maxValue;
    String tableName;

    
    public Bucket() {
		
	}

	public Bucket(String tableName,String bucketName,String[] colNames,String[] colType,String[] ranges) throws DBAppException {
        references= new Vector<Hashtable<String,Object>>();
        overflow=null;
        this.ranges=ranges;
        this.tableName=tableName;
        this.colNames=colNames;
        this.bucketName=bucketName;
        this.colType=colType;
        if(colNames.length!=ranges.length)
            throw new DBAppException("all columns should have a range or a value and vice versa");
        minValue=new Object[colNames.length];
        maxValue=new Object[colNames.length];
        this.setMinMaxValues();
        this.checkPages();
        this.checkIfOverflow();

    }

    private Bucket(String tableName){
        references = new Vector<Hashtable<String,Object>>();
        overflow=null;
        ranges=null;
        colNames=null;
        minValue=null;
        maxValue=null;
        this.tableName=tableName;
    }

    private void setMinMaxValues() {
        //this method will parse the string range input into min and max values and store them in the arrays above

        /*on each entry to the ranges array check if it is a range or a set value,
          if set value store in min and max*/

        for (int i = 0; i < ranges.length; i++) {
            //split to infer type and then to check if range or set value
            String[] range = ranges[i].split(" - ");
            String lowerBound = range[0];

            //infer type
//            Pattern integerRegex = Pattern.compile("\\d");
//            Pattern doubleRegex = Pattern.compile("\\d.[\\d]+");
//            Pattern dateRegex = Pattern.compile("[Sat|Sun|Mon|Tue|Wed|Thu|Fri] [Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec]");
//            Matcher integerMatcher = integerRegex.matcher(lowerBound);
//            Matcher doubleMatcher = doubleRegex.matcher(lowerBound);
//            Matcher dateMatcher = dateRegex.matcher(lowerBound);
//            boolean isInteger = integerMatcher.matches();
//            boolean isDouble = doubleMatcher.matches();
//            boolean isDate = dateMatcher.find();
//            String type = "java.lang.String";
//            if(isDate)
//                type="java.util.Date";
//            else if(isDouble)
//                type="java.lang.Double";
//            else if(isInteger)
//                type="java.lang.Integer";

            String type = colType[i];


            //check if it is a range or a set value and set min and max accordingly
            boolean isRange = (range.length == 2) ? true : false;
            if (isRange) {
                String upperBound = range[1];
                switch (type){
                    case "java.lang.String":
                        this.stringSetRange(i,lowerBound,upperBound);
                        break;
                    case "java.lang.Integer":
                        this.intSetRange(i,lowerBound,upperBound);
                        break;
                    case "java.lang.Double":
                        this.doubleSetRange(i,lowerBound,upperBound);
                        break;
                    case "java.util.Date":
                        this.dateSetRange(i,lowerBound,upperBound);
                        break;
                }
            } else {
                switch (type){
                    case "java.lang.String":
                        this.stringSetRange(i,lowerBound,lowerBound);
                        break;
                    case "java.lang.Integer":
                        this.intSetRange(i,lowerBound,lowerBound);
                        break;
                    case "java.lang.Double":
                        this.doubleSetRange(i,lowerBound,lowerBound);
                        break;
                    case "java.util.Date":
                        this.dateSetRange(i,lowerBound,lowerBound);
                        break;
                }
            }
        }

    }
    private void intSetRange(int index,String lowerBound,String upperBound){
        this.maxValue[index] = Double.parseDouble(upperBound);
        this.minValue[index] = Double.parseDouble(lowerBound);
    }
    private void doubleSetRange(int index,String lowerBound,String upperBound){
        this.maxValue[index] = Double.parseDouble(upperBound);
        this.minValue[index] = Double.parseDouble(lowerBound);
    }
    private void dateSetRange(int index,String lowerBound,String upperBound){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String[] lowerDateArr = lowerBound.split(" ");
        String toMin = "";
        toMin+=lowerDateArr[lowerDateArr.length-1]+"-";
        switch(lowerDateArr[1]){
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
        toMin+=lowerDateArr[2];
        Date minDate = null;
        try {
            minDate = format.parse(toMin);
        } catch (ParseException e) {}
        this.minValue[index] = minDate;

        String[] upperDateArr = upperBound.split(" ");
        String toMax = "";
        toMax+=upperDateArr[upperDateArr.length-1]+"-";
        switch(upperDateArr[1]){
            case "Jan":
                toMax+="01-";
                break;
            case "Feb":
                toMax+="02-";
                break;
            case "Mar":
                toMax+="03-";
                break;
            case "Apr":
                toMax+="04-";
                break;
            case "May":
                toMax+="05-";
                break;
            case "Jun":
                toMax+="06-";
                break;
            case "Jul":
                toMax+="07-";
                break;
            case "Aug":
                toMax+="08-";
                break;
            case "Sep":
                toMax+="09-";
                break;
            case "Oct":
                toMax+="10-";
                break;
            case "Nov":
                toMax+="11-";
                break;
            case "Dec":
                toMax+="12-";
                break;
        }
        toMax+=upperDateArr[2];
        Date maxDate = null;
        try {
            maxDate = format.parse(toMax);
        } catch (ParseException e) {}
        this.maxValue[index] = maxDate;

    }
    private void stringSetRange(int index,String lowerBound,String upperBound){
        this.minValue[index]=lowerBound;
        this.maxValue[index]=upperBound;
    }
    private void checkPages() {
        //this method will set references in this bucket to all pages that match the ranges specified in range arrays

        //first off, check how many pages this table has
        File dir = new File("src/main/resources/data/pages/");
        int noOfPages = 0;
        File[] allPages = dir.listFiles();
        if (allPages != null) {
            for (int i = 0; i < allPages.length; i++) {
                String pageName = allPages[i].getName();
                if (pageName.length() >= this.tableName.length()) {
                    String tableName = pageName.substring(0, this.tableName.length());
                    if (tableName.equals(this.tableName)) {
                        noOfPages++;
                    }
                }
            }
        }

        //for each page in the table, loop over the elements and if they satisfy the range point to the page,row,and indices in order
        for(int i=0;i<noOfPages;i++){
            //loading the page
            Vector<Hashtable<String,Object>> page = new Vector<Hashtable<String,Object>>();
            String filename = "src/main/resources/data/pages/" + this.tableName + i + ".ser";

            try
            {
                // Reading the object from a file
                FileInputStream file = new FileInputStream(filename);
                ObjectInputStream in = new ObjectInputStream(file);

                // Method for deserialization of object
                page = (Vector<Hashtable<String, Object>>)in.readObject();

                in.close();
                file.close();
            } catch (ClassNotFoundException | IOException e1) {
                e1.printStackTrace();
            }

            //for every element in the page,check if the key is one being indexed,if it is then check if in range and add to reference
            element:
            for(int j=0;j<page.size();j++){
                //loop over each element
                //store this element in thisElement, nice commenting skills man
                Hashtable<String,Object> thisElement = page.get(j);
                ArrayList<String> keys = Collections.list(thisElement.keys());
                //loop over the keys
                for(int k=0;k<keys.size();k++){
                    //loop over the columns in the index
                    for(int l=0;l<colNames.length;l++){
                        //if this indexing key is not in the element being checked, go to the next element
                        if(!keys.contains(colNames[l]))
                            continue element;
                        //if the column exists but is not in the range specified, go to next element
                        else{
                            Object thisKey = thisElement.get(colNames[l]);
                            if(thisKey instanceof Integer){
                                if(!((Integer)thisKey>=(Double)minValue[l]) || !((Integer)thisKey<(Double)maxValue[l])){
                                    Double thisKeyD = (double)(int)thisKey;
                                    if(!(thisKeyD.equals(maxValue[l])  && thisKeyD.equals(minValue[l]) ))
                                        continue element;
                                }
                            }
                            else if(thisKey instanceof Double){
                                if(!((Double)thisKey>=(Double)minValue[l]) || !((Double)thisKey<(Double)maxValue[l])){
                                    if(!(thisKey.equals(maxValue[l])  && thisKey.equals(minValue[l]) ))
                                        continue element;
                                }
                            }
                            else if(thisKey instanceof String){
                                int beforeMax = ((String) thisKey).compareTo((String)maxValue[l]);
                                int afterMin =  ((String) thisKey).compareTo((String)minValue[l]);
                                if(!(beforeMax<=0) || !(afterMin>=0)){
                                    if(beforeMax!=0 && afterMin!=0)
                                        continue element;
                                }
                            }
                            else if(thisKey instanceof Date){
                                int beforeMax = ((Date) thisKey).compareTo((Date)maxValue[l]);
                                int afterMin =  ((Date) thisKey).compareTo((Date)minValue[l]);
                                if(!(beforeMax<0) || !(afterMin>=0)){
                                    if(beforeMax!=0 && afterMin!=0)
                                        continue element;
                                }
                            }
                        }
                    }
                }
                //if you got here, then this element has all keys in the index within the range specified
                //add it to the bucket

                Hashtable<String,Object> reference = new Hashtable<String,Object>();
                reference.put("Page",i);
                reference.put("Index",page.indexOf(thisElement));
                for(int l=0;l<colNames.length;l++){
                    reference.put(colNames[l],thisElement.get(colNames[l]));
                }
                references.add(reference);

//                String reference =" Page: " + i + ", " + "Index: " + page.indexOf(thisElement) + ", ";
//                for(int l=0;l<colNames.length;l++){
//                    reference += colNames[l] +": " + thisElement.get(colNames[l].toString()) + ",";
//                }
//
//                reference = reference.substring(0,reference.length()-1);
//                references.add(reference);
            }


        }

    }
    private void checkIfOverflow(){
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
        int maxInBucket =  Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

        if(references.size()>maxInBucket){
            if(overflow == null)
                overflow = new Bucket(this.tableName);
            while(references.size()>maxInBucket){
                overflow.references.add(0,this.references.remove(this.references.size()-1));
            }
            overflow.checkIfOverflow();
        }
    }


    //the select queries, should return a vector with page number and index in this page
    public Vector<int[]> selectFromBucketNotEquals(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                valid = thisValue.equals(value[k]);
            }
            if(!valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketNotEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }
    public Vector<int[]> selectFromBucketEquals(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                valid = thisValue.equals(value[k]);
            }
            if(valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }
    public Vector<int[]> selectFromBucketLessThan(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                int compare = 0;
                if(thisValue instanceof java.lang.String)
                    compare = ((String) thisValue).compareTo((String)value[k]);
                else if(thisValue instanceof java.util.Date)
                    compare = ((Date) thisValue).compareTo((Date)value[k]);
                else if(thisValue instanceof java.lang.Integer)
                    compare = (((Integer)thisValue) < ((Integer)value[k])) ? -1:1;
                else if(thisValue instanceof java.lang.Double)
                    compare = (((Double)thisValue) < ((Double)value[k])) ? -1:1;
               valid = compare < 0;
            }
            if(valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }
    public Vector<int[]> selectFromBucketGreaterThan(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                int compare = 0;
                if(thisValue instanceof java.lang.String)
                    compare = ((String) thisValue).compareTo((String)value[k]);
                else if(thisValue instanceof java.util.Date)
                    compare = ((Date) thisValue).compareTo((Date)value[k]);
                else if(thisValue instanceof java.lang.Integer)
                    compare = (((Integer)thisValue) <= ((Integer)value[k])) ? -1:1;
                else if(thisValue instanceof java.lang.Double)
                    compare = (((Double)thisValue) <= ((Double)value[k])) ? -1:1;
                valid = compare > 0;
            }
            if(valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }
    public Vector<int[]> selectFromBucketLessThanOrEqual(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                int compare = 0;
                if(thisValue instanceof java.lang.String)
                    compare = ((String) thisValue).compareTo((String)value[k]);
                else if(thisValue instanceof java.util.Date)
                    compare = ((Date) thisValue).compareTo((Date)value[k]);
                else if(thisValue instanceof java.lang.Integer)
                    compare = (((Integer)thisValue) <= ((Integer)value[k])) ? 0:1;
                else if(thisValue instanceof java.lang.Double)
                    compare = (((Double)thisValue) <= ((Double)value[k])) ? 0:1;
                valid = compare <= 0;
            }
            if(valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }
    public Vector<int[]> selectFromBucketGreaterThanOrEqual(String[] colNames, Object[] value){
        Vector<int[]> positions= new Vector<int[]>();
        for(int i=0;i<references.size();i++){
            Hashtable thisReference = references.get(i);
            boolean valid = true;
            for(int k=0;k<colNames.length;k++){
                Object thisValue = thisReference.get(colNames[k]);
                int compare = 0;
                if(thisValue instanceof java.lang.String)
                    compare = ((String) thisValue).compareTo((String)value[k]);
                else if(thisValue instanceof java.util.Date)
                    compare = ((Date) thisValue).compareTo((Date)value[k]);
                else if(thisValue instanceof java.lang.Integer)
                    compare = (((Integer)thisValue) <= ((Integer)value[k])) ? -1:1;
                else if(thisValue instanceof java.lang.Double)
                    compare = (((Double)thisValue) <= ((Double)value[k])) ? -1:1;
                valid = compare >= 0;
            }
            if(valid){
                int[] position = new int[2];
                position[0] = (int) thisReference.get("Page");
                position[1] = (int) thisReference.get("Index");
                positions.add(position);
            }
        }
        Vector<int[]> overflowValues = null;
        if(this.overflow!=null)
            overflowValues = this.overflow.selectFromBucketEquals(colNames,value);


        if(overflowValues!=null){
            while(!overflowValues.isEmpty()){
                positions.add(overflowValues.remove(0));
            }
        }

        return positions;
    }




}