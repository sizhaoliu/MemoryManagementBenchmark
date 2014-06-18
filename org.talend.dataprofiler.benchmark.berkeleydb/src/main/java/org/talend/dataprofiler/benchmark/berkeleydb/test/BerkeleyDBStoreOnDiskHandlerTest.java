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
        berkeLeyMap.initBerkeleyDbEnv();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData(100, 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList.toArray(), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());
        // put same key again
        valueList.set(0, 2l);
        berkeLeyMap.putData(keyList.toArray(), valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());// the size of map is same
        Assert.assertEquals(2l, berkeLeyMap.getData(keyList).get(0));// use same key to get vaule and it should be
                                                                     // changed to new one
        // if the key is null will throw a NullPointerExcetion
        Throwable t = null;
        try {
            berkeLeyMap.putData(null, valueList);
        } catch (Exception e) {
            t = e;
        }
        assertNotNull(t);
        assertTrue(t instanceof NullPointerException);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());// the size of map should not be changed
        berkeLeyMap.clear();
        Assert.assertEquals(0, berkeLeyMap.getMapSize());// after clear the size of the map should be zero
        berkeLeyMap.close();

    }

    /**
     * Test method for {@link org.talend.berkeleydb.BerkeleyDBStoreOnDiskHandler#getData(java.lang.Object[])}.
     */
    @Test
    public void testGetData() {
        BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
        berkeLeyMap.initBerkeleyDbEnv();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData(100, 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList, valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());
        // get a value by not exist key will retrun a empty list
        List<Object> data = berkeLeyMap.getData(101);
        Assert.assertEquals(0, data.size());

        // if the key is null will throw a NullPointerExcetion
        Throwable t = null;
        try {
            berkeLeyMap.getData(null);
        } catch (Exception e) {
            t = e;
        }
        assertNotNull(t);
        assertTrue(t instanceof NullPointerException);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());// the size of map should not be changed
        berkeLeyMap.clear();
        Assert.assertEquals(0, berkeLeyMap.getMapSize());// after clear the size of the map should be zero
        berkeLeyMap.close();
    }

    /**
     * Test method for {@link org.talend.berkeleydb.BerkeleyDBStoreOnDiskHandler#removeData(java.lang.Object[])}.
     */
    @Test
    public void testRemoveData() {
        BerkeleyDBStoreOnDiskHandler berkeLeyMap = new BerkeleyDBStoreOnDiskHandler();
        berkeLeyMap.initBerkeleyDbEnv();
        berkeLeyMap.putData("name1", "key1");
        berkeLeyMap.putData(100, 1);
        List<Object> keyList = new ArrayList<Object>();
        List<Object> valueList = new ArrayList<Object>();
        keyList.add("id1");
        keyList.add("name1");
        valueList.add(1l);
        berkeLeyMap.putData(keyList, valueList);
        Assert.assertEquals(3, berkeLeyMap.getMapSize());

        // remove one data which key is 100
        berkeLeyMap.removeData(100);
        List<Object> data = berkeLeyMap.getData(100);
        Assert.assertEquals(0, data.size());
        Assert.assertEquals(2, berkeLeyMap.getMapSize());

        // remove a data which can not find in the map
        berkeLeyMap.removeData(101);
        Assert.assertEquals(2, berkeLeyMap.getMapSize());

        // if the key is null will throw a NullPointerExcetion
        Throwable t = null;
        try {
            berkeLeyMap.removeData(null);
        } catch (Exception e) {
            t = e;
        }
        assertNotNull(t);
        assertTrue(t instanceof NullPointerException);
    }

}
