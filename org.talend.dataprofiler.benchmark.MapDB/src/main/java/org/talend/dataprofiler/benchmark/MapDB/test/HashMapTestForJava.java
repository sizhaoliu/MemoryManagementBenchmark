package org.talend.dataprofiler.benchmark.MapDB.test;

	import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

	import junit.framework.Assert;

	import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class HashMapTestForJava {
	


		
		private Long distinctCount = 0l;

	    private Long rowCount = 0l;

	    private Long uniqueCount = 0l;

	    private Long duplicateCount = 0l;
		@Test
	    public void testHugeDataForTreeMap() throws UnsupportedEncodingException{
	    	setStartT();
	    	Map<String,Long> tempMap=new HashMap();
//	    	 DB db = DBMaker.newTempFileDB().asyncWriteFlushDelay(100).closeOnJvmShutdown().transactionDisable().make();
//	    	 ConcurrentNavigableMap<String, Long> dbMap = db.createTreeMap("test").keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.LONG).make();
	    	
	    	for(int index=0;index<7e6;index++){
	    		try{
	    		for(String[] dataItem:initRandomData()){
	    			String keyStr=ConvertToKey(dataItem);
	    			 Long frequency = tempMap.get(keyStr);
	    			if(frequency!=null){
	    				frequency++;
	    			}else{
	    				frequency=1l;
	    			}
	    			
	    				tempMap.put(keyStr, frequency);
	    		}
	    			}catch(OutOfMemoryError e){
	    				System.out.println("index="+index);
	    				System.out.println(e.getMessage());
	    				ellipseT();
	    				throw e;
	    			}
	    		
	    	}
//	    	for(int index=0;index<3.75e4;index++){
//	    		for(String[] dataItem:initData()){
//	    			Long frequency = dbMap.get(ConvertToKey(dataItem));
//	    			if(frequency!=null){
//	    				frequency++;
//	    			}else{
//	    				frequency=1l;
//	    			}
//	    			dbMap.put(ConvertToKey(dataItem), frequency);
//	    		}
//	    	}
	    	ellipseT();
	    	computeResult(tempMap);
	    	ellipseT();
//	    	Assert.assertEquals(10l, distinctCount.longValue());
	    	Assert.assertEquals(7000000l, rowCount.longValue());
	    	Assert.assertEquals(uniqueCount.longValue(), distinctCount.longValue()-duplicateCount.longValue());
//	    	Assert.assertEquals(0l, uniqueCount.longValue());
//	    	Assert.assertEquals(10l, duplicateCount.longValue());
//	    	db.close();
	    }
	    
	    private void computeResult(Map<String, Long> tempMap) throws UnsupportedEncodingException{
	    	for(String keyArrays:tempMap.keySet()){
	    		Long frequency = tempMap.get(keyArrays);
	    		rowCount+=frequency;
	    		distinctCount++;
	    		if(frequency==1){
	    			uniqueCount++;
	    		}else{
	    			duplicateCount++;
	    		}
	    		
	    	}
	    }
	    
	    private List<String[]> initData() {
	        List<String[]> returnList = new ArrayList<String[]>();
	        returnList.add(new String[] { "name1", "id1", "city1" });//	1	
	        returnList.add(new String[] { "name6", "id6", "city6" });//						6
	        returnList.add(new String[] { "name4", "id4", "city4" });//				4
	        returnList.add(new String[] { "name5", "id5", "city5" });//					5
	        returnList.add(new String[] { "name8", "id8", "city8" });//								8
	        returnList.add(new String[] { "name3", "id3", "city3" });//			3
	        returnList.add(new String[] { "name7", "id7", "city7" });//							7
	        returnList.add(new String[] { "name5", "id5", "city5" });//					5
	        returnList.add(new String[] { "name2", "id2", "city2" });//		2
	        returnList.add(new String[] { "name9", "id9", "city9" });//									9
	        returnList.add(new String[] { "name4", "id4", "city4" });//				4
	        returnList.add(new String[] { "name3", "id3", "city3" });//			3
	        returnList.add(new String[] { "name5", "id5", "city5" });//					5
	        returnList.add(new String[] { "name0", "id0", "city0" });//0
	        returnList.add(new String[] { "name4", "id4", "city4" });//				4
	        returnList.add(new String[] { "name2", "id2", "city2" });//		2
	        returnList.add(new String[] { "name5", "id5", "city5" });//					5
	        returnList.add(new String[] { "name3", "id3", "city3" });//			3
	        returnList.add(new String[] { "name4", "id4", "city4" });//				4
	        returnList.add(new String[] { "name5", "id5", "city5" });//					5
	        return returnList;
	    }
	    private List<String[]> initRandomData() {
	    	List<String[]> returnList = new ArrayList<String[]>();
	    	returnList.add(new String[] { "name"+Math.random(), "id"+Math.random(), "city"+Math.random() });//	1	
	    	return returnList;
	    }
	    
	    
	    static long startT = 0;

	    static long endT = 0;
	    public static void setStartT() {
	        startT = System.currentTimeMillis();
	    }

	    public static long ellipseT() {
	        endT = System.currentTimeMillis();
	        long consumeT = endT - startT;
	        System.out.println("consume time :" + consumeT / 1000 + " second");
	        return consumeT / 1000;
	    }
	    private String ConvertToKey(String[] input){
	 	   if(input==null){
	 		   return "";
	 	   }
	 	   StringBuffer strBuf=new StringBuffer();
	 	  for(String str:input){
	 		  strBuf.append(str);
	 		  	  }
	 	  return strBuf.toString();
	    }


}
