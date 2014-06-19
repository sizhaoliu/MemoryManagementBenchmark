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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.talend.dataprofiler.benchmark.berkeleydb.BerkeleyDBStoreOnDiskHandler;

/**
 * created by talend on Jun 18, 2014 Detailled comment
 * 
 */
public class BerkeleyDBStoreOnDiskHandlerTest {

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
    @Test
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
    @Test
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
    @Test
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

}
