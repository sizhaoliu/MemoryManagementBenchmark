// ============================================================================
//
// Copyright (C) 2006-2014 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.dataprofiler.benchmark.berkeleydb.test;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.talend.dataprofiler.benchmark.berkeleydb.BerkeleyDBStoreOnDiskHandler;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * created by talend on Jun 18, 2014 Detailled comment
 * 
 */
public class BerkeleyDBStoreOnDiskHandlerTest {
	 private Long distinctCount = 0l;

	    private Long rowCount = 0l;

	    private Long uniqueCount = 0l;

	    private Long duplicateCount = 0l;
    /**
     * DOC talend Comment method "setUpBeforeClass".
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * DOC talend Comment method "tearDownAfterClass".
     * 
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * DOC talend Comment method "setUp".
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {

    }

    /**
     * DOC talend Comment method "tearDown".
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link org.talend.berkeleydb.BerkeleyDBStoreOnDiskHandler#putData(java.util.List)}.
     */
//    @Test
    public void testPutData() {
        BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData("100", 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList.toArray(new String[keyList.size()]), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());
        // put same key again
        valueList.set(0, 2l);
        berkeLeyMap.putData(keyList.toArray(new String[0]), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());// the size of map is same
        Assert.assertEquals(2l, berkeLeyMap.getData(keyList.toArray(new String[0])).get(0));// use same key to get vaule and it should be
                                                                     // changed to new one
        // put two null
        berkeLeyMap.putData((String)null, valueList);
        berkeLeyMap.putData((String)null, valueList);
        Assert.assertEquals(4, berkeLeyMap.getMapSize());// put two null the size of map should add one only from 3 to 4
        
        berkeLeyMap.clear();
        berkeLeyMap.initBerkeleyDbEnv();
        Assert.assertEquals(0, berkeLeyMap.getMapSize());// after clear the size of the map should be zero
        berkeLeyMap.close();

    }

    /**
     * Test method for {@link org.talend.berkeleydb.BerkeleyDBStoreOnDiskHandler#getData(java.lang.Object[])}.
     */
//    @Test
    public void testGetData() {
        BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData("100", 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList.toArray(new String[keyList.size()]), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());
        // get a value by not exist key will retrun a null
        List<Object> data = berkeLeyMap.getData("101");
        Assert.assertEquals(null, data);

        // put a null
        berkeLeyMap.putData((String)null, valueList);
        Assert.assertEquals(4, berkeLeyMap.getMapSize());// the size of map should add one
      
        // get a null
        data =berkeLeyMap.getData((String)null);
        Assert.assertEquals(1, data.size());
        berkeLeyMap.clear();
        berkeLeyMap.initBerkeleyDbEnv();
        Assert.assertEquals(0, berkeLeyMap.getMapSize());// after clear the size of the map should be zero
        berkeLeyMap.close();
    }

    /**
     * Test method for {@link org.talend.berkeleydb.BerkeleyDBStoreOnDiskHandler#removeData(java.lang.Object[])}.
     */
//    @Test
    public void testRemoveData() {
        BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData("100", 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList.toArray(new String[keyList.size()]), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());

        // remove one data which key is 100
        boolean isSuccess = berkeLeyMap.removeData("100");
        Assert.assertEquals(true,isSuccess);
        List<Object> data = berkeLeyMap.getData("100");
        Assert.assertEquals(null, data);
        Assert.assertEquals(2, berkeLeyMap.getMapSize());

        // remove a data which can not find in the map
        isSuccess =berkeLeyMap.removeData("101");
        Assert.assertEquals(false,isSuccess);
        Assert.assertEquals(2, berkeLeyMap.getMapSize());

        //remove a null
        isSuccess =berkeLeyMap.removeData((String)null);
        Assert.assertEquals(false,isSuccess);
        Assert.assertEquals(2, berkeLeyMap.getMapSize());
       
        // put a null
        berkeLeyMap.putData((String)null, valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());// the size of map should added one
        
        //remove the null again
        isSuccess =berkeLeyMap.removeData((String)null);
        Assert.assertEquals(true,isSuccess);
        Assert.assertEquals(2, berkeLeyMap.getMapSize());// the size of map should added one
        
        //clear the map
        berkeLeyMap.clear();
        berkeLeyMap.initBerkeleyDbEnv();//init again at here is used to get the size of map, normal case we don't need init again
        Assert.assertEquals(0, berkeLeyMap.getMapSize());// after clear the size of the map should be zero
        berkeLeyMap.close();
    }
    
    public void testHugeData16000() throws UnsupportedEncodingException{
    	setStartT();
    	BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
    	for(int index=0;index<10000;index++){
    		for(String[] dataItem:initData()){
    			List<Object> data = berkeLeyMap.getData(dataItem);
    			int frequency=1;
    			if(data!=null){
    				frequency=Integer.valueOf(berkeLeyMap.getData(dataItem).get(0).toString());
    				frequency++;
    			}
    			berkeLeyMap.putData(dataItem, frequency);
    		}
    	}
    	ellipseT();
    	computeResult(berkeLeyMap);
    	ellipseT();
    	Assert.assertEquals(10l, distinctCount.longValue());
    	Assert.assertEquals(160000l, rowCount.longValue());
    	Assert.assertEquals(0l, uniqueCount.longValue());
    	Assert.assertEquals(10l, duplicateCount.longValue());
    }
    @Test
    public void testHugeData10000() throws UnsupportedEncodingException{
    	setStartT();
    	BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
    	for(int index=0;index<100000;index++){
    		for(String[] dataItem:initRandomData()){
    			List<Object> data = berkeLeyMap.getData(dataItem);
    			int frequency=1;
    			if(data!=null){
    				frequency=Integer.valueOf(berkeLeyMap.getData(dataItem).get(0).toString());
    				frequency++;
    			}
    			berkeLeyMap.putData(dataItem, frequency);
    		}
    	}
    	ellipseT();
    	computeResult(berkeLeyMap);
    	ellipseT();
//    	Assert.assertEquals(10l, distinctCount.longValue());
    	Assert.assertEquals(100000l, rowCount.longValue());
    	Assert.assertEquals(uniqueCount.longValue(), distinctCount.longValue()-duplicateCount.longValue());
//    	Assert.assertEquals(0l, uniqueCount.longValue());
//    	Assert.assertEquals(10l, duplicateCount.longValue());
    }
    
    private void computeResult(BerkeleyDBStoreOnDiskHandler berkeLeyMap) throws UnsupportedEncodingException{
    	Cursor cursor = berkeLeyMap.getCursor();
    	DatabaseEntry key=new DatabaseEntry();
    	DatabaseEntry value=new DatabaseEntry();
    	TupleBinding<String> stringBinding = TupleBinding.getPrimitiveBinding(String.class);
    	while(cursor.getNext(key, value, LockMode.DEFAULT)== OperationStatus.SUCCESS){
    		String keyStr = stringBinding.entryToObject(key);
    		List<Object> dataList = berkeLeyMap.getData(keyStr);
    		Integer frequency = Integer.valueOf(dataList.get(0).toString());
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
        returnList.add(new String[] { "name2", "id2", "city2" });//		2
        returnList.add(new String[] { "name9", "id9", "city9" });//									9
        returnList.add(new String[] { "name4", "id4", "city4" });//				4
        returnList.add(new String[] { "name3", "id3", "city3" });//			3
        returnList.add(new String[] { "name0", "id0", "city0" });//0
        returnList.add(new String[] { "name4", "id4", "city4" });//				4
        returnList.add(new String[] { "name2", "id2", "city2" });//		2
        returnList.add(new String[] { "name3", "id3", "city3" });//			3
        returnList.add(new String[] { "name4", "id4", "city4" });//				4
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

}
