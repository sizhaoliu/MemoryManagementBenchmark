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
package org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.handler.ColumnSetStoreOnDiskHandler;


/**
 * created by talend on Jun 19, 2014 Detailled comment
 * 
 */
public class ColumnSetStoreOnDiskHandlerTest {


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
     * Test method for {@link javatest.nio.handler.ColumnSetStoreOnDiskHandler#handleRow(java.lang.Object[])}.
     * 
     * @throws Exception
     */
    @Test
    public void testHandleRow() throws Exception {
        ColumnSetStoreOnDiskHandler colSetHandler = null;
        colSetHandler = new ColumnSetStoreOnDiskHandler("d:\\tmp\\JEDB2\\", 3);
        colSetHandler.beginQuery();
        List<Object[]> inputData = initData();
        for (Object[] element : inputData) {
            colSetHandler.handleRow(element);
        }
        Assert.assertEquals(16l, colSetHandler.getRowCount().longValue());
        Assert.assertEquals(10l, colSetHandler.getDistinctCount().longValue());
        colSetHandler.endQuery();
        colSetHandler.computeResult();
        Assert.assertEquals(3l, colSetHandler.getDuplicateCount().longValue());
        Assert.assertEquals(7l, colSetHandler.getUniqueCount().longValue());
        boolean isDeletePersistenceFile = colSetHandler.deletePersistenceFile();
        Assert.assertEquals(true, isDeletePersistenceFile);
        
    }

    private List<Object[]> initData() {
        List<Object[]> returnList = new ArrayList<Object[]>();
        returnList.add(new Object[] { "name1", "id1", "city1" });//	1	
        returnList.add(new Object[] { "name6", "id6", "city6" });//						6
        returnList.add(new Object[] { "name4", "id4", "city4" });//				4
        returnList.add(new Object[] { "name5", "id5", "city5" });//					5
        returnList.add(new Object[] { "name8", "id8", "city8" });//								8
        returnList.add(new Object[] { "name3", "id3", "city3" });//			3
        returnList.add(new Object[] { "name7", "id7", "city7" });//							7
        returnList.add(new Object[] { "name2", "id2", "city2" });//		2
        returnList.add(new Object[] { "name9", "id9", "city9" });//									9
        returnList.add(new Object[] { "name4", "id4", "city4" });//				4
        returnList.add(new Object[] { "name3", "id3", "city3" });//			3
        returnList.add(new Object[] { "name0", "id0", "city0" });//0
        returnList.add(new Object[] { "name4", "id4", "city4" });//				4
        returnList.add(new Object[] { "name2", "id2", "city2" });//		2
        returnList.add(new Object[] { "name3", "id3", "city3" });//			3
        returnList.add(new Object[] { "name4", "id4", "city4" });//				4
        return returnList;
    }

}
