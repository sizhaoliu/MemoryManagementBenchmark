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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

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
        berkeleyDbEnv.setup(new File("d:\\tmp\\JEDB2"), false);
    }

    /**
     * 
     * Put key and value into file BD. If key is same the old value will be rewrited
     * 
     * @param key
     * @param value
     */
    public void putData(String key, Object value) {
        DataListRow valueListRow = new DataListRow();
        valueListRow.getKeys().add(key);
    	valueListRow.getValue().add(value);
    	
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<String> stringBinding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        stringBinding.objectToEntry(key, keyEntry);
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
    public void putData(String[] key, Object value) {
    	String keyString = ConvertToKey(key);
    	DataListRow valueListRow = new DataListRow();
    	valueListRow.getKeys().addAll(Arrays.asList(key));
    	valueListRow.getValue().add(value);
    	
    	StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
    	EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
    	TupleBinding<String> integerBinding = TupleBinding.getPrimitiveBinding(String.class);
    	DatabaseEntry keyEntry = new DatabaseEntry();
    	DatabaseEntry valueEntry = new DatabaseEntry();
    	integerBinding.objectToEntry(keyString, keyEntry);
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
    public void putData(String[] key, List<Object> value) {
    	String keyString = ConvertToKey(key);
    	DataListRow valueListRow = new DataListRow();
    	valueListRow.getKeys().addAll(Arrays.asList(key));
    	valueListRow.getValue().addAll(value);
    	StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
    	EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
    	TupleBinding<String> integerBinding = TupleBinding.getPrimitiveBinding(String.class);
    	DatabaseEntry keyEntry = new DatabaseEntry();
    	DatabaseEntry valueEntry = new DatabaseEntry();
    	integerBinding.objectToEntry(keyString, keyEntry);
    	dataBinding.objectToEntry(valueListRow, valueEntry);
    	berkeleyDbEnv.getDataDB().put(null, keyEntry, valueEntry);
    }

    /**
     * 
     * Get the value which key is key
     * 
     * @param key
     * @return null if can not find a value by the key
     */
    public List<Object> getData(String key) {
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<String> integerBinding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        integerBinding.objectToEntry(key, keyEntry);
        if(berkeleyDbEnv.getDataDB().get(null, keyEntry, valueEntry, LockMode.DEFAULT)==OperationStatus.SUCCESS){
        	DataListRow valueListRow = dataBinding.entryToObject(valueEntry);
        	return valueListRow.getValue();
        }else{
        	return null;
        }
    }

    /**
     * 
     * Get the value which key is keyArray
     * 
     * @param keyArray
     * @return null if can not find a value by the keyArray
     */
    public List<Object> getData(String[] keyArray) {
        String keyString = ConvertToKey(keyArray);
        StoredClassCatalog classCatalog = berkeleyDbEnv.getClassCatalog();
        EntryBinding<DataListRow> dataBinding = new SerialBinding<DataListRow>(classCatalog, DataListRow.class);
        TupleBinding<String> stringBinding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        stringBinding.objectToEntry(keyString, keyEntry);
        if(berkeleyDbEnv.getDataDB().get(null, keyEntry, valueEntry, LockMode.DEFAULT)==OperationStatus.SUCCESS){
        	DataListRow valueListRow = dataBinding.entryToObject(valueEntry);
        	return valueListRow.getValue();
        }else{
        	return null;
        }
    }

    /**
     * 
     * Remove the data by special keyArray.
     * 
     * @param keyArray if it is null will cause NullPoiniterException
     * @return true if remove opertaion is success else return false
     */
    public boolean removeData(String[] keyArray) {
        String keyString = ConvertToKey(keyArray);
        DatabaseEntry keyEntry = new DatabaseEntry();
        TupleBinding<String> integerBinding = TupleBinding.getPrimitiveBinding(String.class);
        integerBinding.objectToEntry(keyString, keyEntry);
        if(berkeleyDbEnv.getDataDB().delete(null, keyEntry)==OperationStatus.SUCCESS){
        	return true;
        }
        return false;
    }

    /**
     * 
     * Remove the data by special key.
     * 
     * @param key if it is null will cause NullPoiniterException
     * @return true if remove opertaion is success else return false
     */
    public boolean removeData(String key) {
        DatabaseEntry keyEntry = new DatabaseEntry();
        TupleBinding<String> integerBinding = TupleBinding.getPrimitiveBinding(String.class);
        integerBinding.objectToEntry(key, keyEntry);
        if(berkeleyDbEnv.getDataDB().delete(null, keyEntry)==OperationStatus.SUCCESS){
        	return true;
        }
        return false;
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
   
   public Cursor getCursor(){
	  return berkeleyDbEnv.getDataDB().openCursor(null, null);
   }
}
