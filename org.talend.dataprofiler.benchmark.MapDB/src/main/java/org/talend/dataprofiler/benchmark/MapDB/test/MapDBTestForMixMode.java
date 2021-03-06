package org.talend.dataprofiler.benchmark.MapDB.test;

import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;

import junit.framework.Assert;

import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.mapdb.HTreeMap;
import org.mapdb.Pump;
import org.mapdb.Serializer;
import org.mapdb.Store;
import org.mapdb.StoreHeap;


public class MapDBTestForMixMode {
	
	private Long distinctCount = 0l;

    private Long rowCount = 0l;

    private Long uniqueCount = 0l;

    private Long duplicateCount = 0l;
    
    List<BTreeMap<String, Long>> mapList=new ArrayList<BTreeMap<String, Long>>();
    
	@Test
    public void testHugeDataForTreeMap() throws UnsupportedEncodingException{
    	setStartT();
    	DB fileDb =DBMaker.newTempFileDB().cacheSize(12*1024).mmapFileEnableIfSupported().asyncWriteEnable().closeOnJvmShutdown().transactionDisable().make();
    	 DB db = DBMaker.newMemoryDirectDB().sizeLimit(1).closeOnJvmShutdown().transactionDisable().make();
    	 BTreeMap<String,Long> memoryDbMap = db.createTreeMap("test").keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.LONG).make();
//    	 HTreeMap<String, Long> memoryDbMap = db.createHashMap("test").keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).make();
//    	 HTreeMap<String, Long> fileDbMap= fileDb.createHashMap("test").keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).make();
    	 Store memoryDbStore = Store.forDB(db);
    	for(int index=0;index<1e7;index++){
    		if(memoryDbStore.getSizeLimit()-memoryDbStore.getCurrSize()<=1024*1024*1){
//    			System.out.println(store.calculateStatistics());
    			//copy data to file when the size of memory is too large
    			System.out.println("1111"+memoryDbStore.getCurrSize()/1024);
    			ellipseT();
    			copyDataToFile(memoryDbMap,fileDb);
    			db.close();
    			db = DBMaker.newMemoryDirectDB().sizeLimit(1).closeOnJvmShutdown().transactionDisable().make();
    	    	memoryDbMap = db.createTreeMap("test").keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.LONG).make();
//    	    	memoryDbMap = db.createHashMap("test").keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).make();
    	    	memoryDbStore = Store.forDB(db);
    		}
    		for(String[] dataItem:initRandomData()){
    			String convertToKey = ConvertToKey(dataItem);
    			Long frequency = memoryDbMap.get(convertToKey);
    			if(frequency!=null){
    				frequency++;
    				
    			}else{
    				frequency=1l;
    			}
    			memoryDbMap.put(convertToKey, frequency);
    		}
//    		copyDataToFile(memoryDbMap,fileDb,fileDbMap);
//    		db.close();
//			db = DBMaker.newMemoryDirectDB().sizeLimit(0.5).closeOnJvmShutdown().transactionDisable().make();
//	    	memoryDbMap = db.createTreeMap("test").keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.LONG).make();
//	    	memoryDbStore = Store.forDB(db);
    			
    	}
//    	for(int index=0;index<2.5e5;index++){
//    		if(memoryDbStore.getSizeLimit()-memoryDbStore.getCurrSize()<=1024*1024*1){
////    			System.out.println(store.calculateStatistics());
//    			//copy data to file when the size of memory is too large
//    			System.out.println(memoryDbStore.getCurrSize()/1024);
//    			ellipseT();
//    			copyDataToFile(memoryDbMap,fileDb);
//    			db.close();
//    			db = DBMaker.newMemoryDirectDB().sizeLimit(0.5).closeOnJvmShutdown().transactionDisable().make();
//    			memoryDbMap = db.createTreeMap("test").keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.LONG).make();
////    			memoryDbMap = db.createHashMap("test").keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).make();
//    			memoryDbStore = Store.forDB(db);
//    		}
//    		for(String[] dataItem:initData()){
//    			String convertToKey = ConvertToKey(dataItem);
//    			Long frequency = memoryDbMap.get(convertToKey);
//    			if(frequency!=null){
//    				frequency++;
//    			}else{
//    				frequency=1l;
//    			}
//    			memoryDbMap.put(convertToKey, frequency);
//    		}
//    		
//    	}
    	
    	//copy last data which leave at memory
    	System.out.println(memoryDbStore.getCurrSize()/1024);
		ellipseT();
		copyDataToFile(memoryDbMap,fileDb);
		db.close();
    	ellipseT();
    	computeResult();
    	ellipseT();
//    	Assert.assertEquals(10l, distinctCount.longValue());
    	Assert.assertEquals(10000000l, rowCount.longValue());
    	Assert.assertEquals(uniqueCount.longValue(), distinctCount.longValue()-duplicateCount.longValue());
//    	Assert.assertEquals(0l, uniqueCount.longValue());
//    	Assert.assertEquals(10l, duplicateCount.longValue());
    	fileDb.close();
    	if(!db.isClosed()){
    		db.close();
    	}
    	System.out.println("end");
		ellipseT();
    }
    
    private void copyDataToFile(
    		final BTreeMap<String,Long > dbMap, DB fileDb) {
    	
    	Iterator sortIterator = Pump.sort(dbMap.keySet().iterator(), true, 100000, Collections.reverseOrder(BTreeMap.COMPARABLE_COMPARATOR), // reverse
                // order
                // comparator
    			Serializer.STRING);
    	
    	 Fun.Function1<Long, String> valueExtractor = new Fun.Function1<Long, String>() {

			@Override
			public Long run(String a) {
				Long returnValue=dbMap.get(a);
				Long oldValue =null;
				//find item
				BTreeMap<String, Long> map =findItemFromMapList(a);
				//delete item
				if(map!=null){
					oldValue = map.get(a);
					deleteItemFromMap(a,map);
				}
				
				if(oldValue!=null){
					returnValue=returnValue+oldValue;
				}
				return returnValue;
			}

			private void deleteItemFromMap(String a, BTreeMap<String, Long> map) {
				map.remove(a);
				
			}

			private BTreeMap<String, Long> findItemFromMapList(String a) {
				for(BTreeMap<String, Long> currentMap:mapList){
					if(currentMap.get(a)!=null){
						return currentMap;
					}
				}
				return null;
			}

            
         };
         String randomString = randomString(10);
          BTreeMap<String, Long> make = fileDb.createTreeMap("map"+randomString).pumpSource(sortIterator, valueExtractor)
        	        // .pumpPresort(100000) // for presorting data we could also use this method
        	                .keySerializer(BTreeKeySerializer.STRING).makeOrGet();
          mapList.add(make);
//          fileDbMap.putAll(make);
//          for(String key :fileDbMap.keySet()){
//        	  System.out.println("key="+key);
//        	  System.out.println("value="+fileDbMap.get(key));
//          }
          
	}
//    private BTreeMap<String, Long> copyDataToFile(
//    		BTreeMap<String, Long> dbMap, BTreeMap<String, Long> fileDbMap ) {
//    	
//    	for(String key:dbMap.keySet()){
//    		Long oldValue = fileDbMap.get(key);
//    		if(oldValue==null){
//    			fileDbMap.put(key, dbMap.get(key));
//    		}else{
//    			fileDbMap.put(key, oldValue+dbMap.get(key));
//    		}
//    	}
//    	return fileDbMap;
//    }

	private void computeResult() throws UnsupportedEncodingException{
    	for(BTreeMap<String, Long> oldDbMap:mapList){
		for(String keyArrays:oldDbMap.keySet()){
    		Long frequency = oldDbMap.get(keyArrays);
    		rowCount+=frequency;
    		distinctCount++;
    		if(frequency==1){
    			uniqueCount++;
    		}else{
    			duplicateCount++;
    		}
    		
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
    
    public static String randomString(int size) {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
        StringBuilder b = new StringBuilder(size);
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            b.append(chars.charAt(r.nextInt(chars.length())));
        }
        return b.toString();
    }

}
