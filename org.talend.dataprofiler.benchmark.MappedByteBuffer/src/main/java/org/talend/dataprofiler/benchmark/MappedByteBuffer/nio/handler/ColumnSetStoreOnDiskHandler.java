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
package org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.handler;

import java.io.IOException;
import java.util.Arrays;

import org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.ColumnSetLookupManager;
import org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.ColumnSetRow;


/**
 * created by talend on Jun 9, 2014 Detailled comment
 * 
 */
public class ColumnSetStoreOnDiskHandler extends AbstractStoreOnDiskHandler<ColumnSetRow> {

    private Long distinctCount = 0l;

    private Long rowCount = 0l;

    private Long uniqueCount = 0l;

    private Long duplicateCount = 0l;

    /**
     * DOC talend ColumnSetStoreOnDiskHandler constructor comment.
     * 
     * @param container
     * @param buffSize
     * @throws IOException
     */
    public ColumnSetStoreOnDiskHandler(String container, int buffSize) throws IOException {
        super(container, buffSize);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.talend.dq.analysis.persistent.handler.AbstractStoreOnDiskHandler#initPersistentLookupManager(java.lang.String
     * , int)
     */
    @Override
    protected void initPersistentLookupManager(String container, int buffSize) throws IOException {
        persistentLookupManager = new ColumnSetLookupManager(container,
                new org.talend.designer.components.persistent.IRowCreator<ColumnSetRow>() {

                    public ColumnSetRow createRowInstance() {
                        return new ColumnSetRow();
                    }
                });

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.talend.dq.analysis.persistent.handler.AbstractStoreOnDiskHandler#handleRow(java.lang.Object[])
     */
    @Override
    public void handleRow(Object[] oneRow) throws Exception {
        // Store data on disk
        String[] rowArray = new String[oneRow.length];
        int index = 0;
        for (Object obj : oneRow) {
            rowArray[index++] = obj == null ? null : obj.toString();
        }
        ColumnSetRow columnSetRow = new ColumnSetRow();
        columnSetRow.setRowList(Arrays.asList(rowArray));
        columnSetRow.setCh(((ColumnSetLookupManager) persistentLookupManager).getCh());
        // search in the file
        persistentLookupManager.lookup(columnSetRow);
        if (columnSetRow.getFrequency() > 1) {
        } else {
            this.distinctCount++;
        }
        this.rowCount++;
        persistentLookupManager.put(columnSetRow);
    }
    
    public void computeResult() throws IOException{
    	persistentLookupManager.initGet();
    	while(persistentLookupManager.hasNext()){
                ColumnSetRow nextColumnSetRow =persistentLookupManager.next();
                if (nextColumnSetRow.getFrequency()==1) {
                	uniqueCount++;
                }else{
                	duplicateCount++;
                }
    	}
    	persistentLookupManager.endGet();
    }

    /**
     * Getter for rowCount.
     * 
     * @return the rowCount
     */
    public Long getRowCount() {
        return this.rowCount;
    }

    /**
     * Getter for distinctCount.
     * 
     * @return the distinctCount
     */
    public Long getDistinctCount() {
        return this.distinctCount;
    }

	public Long getDuplicateCount() {
		return duplicateCount;
	}

	public Long getUniqueCount() {
		return uniqueCount;
	}
	
	public boolean deletePersistenceFile(){
		return ((ColumnSetLookupManager) persistentLookupManager).deletePersistenceFile();
	}
	
    
    

}
