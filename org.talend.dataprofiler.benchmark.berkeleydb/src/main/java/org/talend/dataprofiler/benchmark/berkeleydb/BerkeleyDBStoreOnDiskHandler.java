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
package org.talend.dataprofiler.benchmark.berkeleydb;

import java.io.File;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;

/**
 * created by talend on Jun 17, 2014 Detailled comment
 * 
 */
public class BerkeleyDBStoreOnDiskHandler {

    private BerkeleyDbEnv berkeleyDbEnv = null;

    public BerkeleyDBStoreOnDiskHandler() {
        initBerkeleyDbEnv();
    }

    public void initBerkeleyDbEnv() {
        berkeleyDbEnv = new BerkeleyDbEnv();
        berkeleyDbEnv.setup(new File("d:\\tmp\\JEDB1"), false);
    }

    /**
     * 
     * Put key and value into file BD. If key is same the old value will be rewrited
     * 
     * @param key
     * @param value
     */
    public void putData(Object key, Object value) {
        int hashCode = key.hashCode();
        DataListRow valueListRow = new DataListRow(value);
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        integerBinding.objectToEntry(hashCode, keyEntry);
        dataBinding.objectToEntry(valueListRow, valueEntry);
        berkeleyDbEnv.getDataDB().put(null, keyEntry, valueEntry);
    }
    /**
     * 
     * Put key and value into file BD. If key is same the old value will be rewrited
     * 
     * @param key
     * @param value
     */
    public void putData(Object[] key, Object value) {
    	int hashCode = key.hashCode();
    	DataListRow valueListRow = new DataListRow(value);
    	StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
    	EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
    	TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
    	DatabaseEntry keyEntry = new DatabaseEntry();
    	DatabaseEntry valueEntry = new DatabaseEntry();
    	integerBinding.objectToEntry(hashCode, keyEntry);
    	dataBinding.objectToEntry(valueListRow, valueEntry);
    	berkeleyDbEnv.getDataDB().put(null, keyEntry, valueEntry);
    }

    /**
     * 
     * Get the value which key is key
     * 
     * @param key
     * @return empty list if can not find a value by the key
     */
    public List<Object> getData(Object key) {
        int hashCode = key.hashCode();
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        integerBinding.objectToEntry(hashCode, keyEntry);
        berkeleyDbEnv.getDataDB().get(null, keyEntry, valueEntry, LockMode.DEFAULT);
        DataListRow valueListRow = dataBinding.entryToObject(valueEntry);
        return valueListRow.getData();
    }

    /**
     * 
     * Get the value which key is keyArray
     * 
     * @param keyArray
     * @return empty list if can not find a value by the keyArray
     */
    public List<Object> getData(Object[] keyArray) {
        int hashCode = keyArray.hashCode();
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        integerBinding.objectToEntry(hashCode, keyEntry);
        berkeleyDbEnv.getDataDB().get(null, keyEntry, valueEntry, LockMode.DEFAULT);
        DataListRow valueListRow = dataBinding.entryToObject(valueEntry);
        return valueListRow.getData();
    }

    /**
     * 
     * Remove the data by special keyArray.
     * 
     * @param keyArray if it is null will cause NullPoiniterException
     */
    public void removeData(Object[] keyArray) {
        int hashCode = keyArray.hashCode();
        DatabaseEntry keyEntry = new DatabaseEntry();
        TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        integerBinding.objectToEntry(hashCode, keyEntry);
        berkeleyDbEnv.getDataDB().delete(null, keyEntry);
    }

    /**
     * 
     * Remove the data by special key.
     * 
     * @param key if it is null will cause NullPoiniterException
     */
    public void removeData(Object key) {
        int hashCode = key.hashCode();
        DatabaseEntry keyEntry = new DatabaseEntry();
        TupleBinding<Integer> integerBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        integerBinding.objectToEntry(hashCode, keyEntry);
        berkeleyDbEnv.getDataDB().delete(null, keyEntry);
    }

    /**
     * 
     * Get the size of the map
     * 
     * @return
     */
    public long getMapSize() {
        return berkeleyDbEnv.getDataDB().count();
    }

    /**
     * Clear the map data
     */
    public void clear() {
        berkeleyDbEnv.clearDatabase();
    }

    /**
     * 
     * Close the map it should be done after use the map
     */
    public void close() {
        berkeleyDbEnv.close();
    }
}
