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

import org.talend.dataprofiler.benchmark.MappedByteBuffer.nio.AbstractRow;
import org.talend.designer.components.lookup.persistent.IPersistentLookupManager;




/**
 * created by talend on Jun 9, 2014 Detailled comment
 * 
 */
public abstract class AbstractStoreOnDiskHandler<T extends AbstractRow<T>> {

    protected IPersistentLookupManager<T> persistentLookupManager;

    /**
     * Getter for persistentLookupManager.
     * 
     * @return the persistentLookupManager
     */
    public IPersistentLookupManager<T> getPersistentLookupManager() {
        return this.persistentLookupManager;
    }

    /**
     * Sets the persistentLookupManager.
     * 
     * @param persistentLookupManager the persistentLookupManager to set
     */
    public void setPersistentLookupManager(IPersistentLookupManager<T> persistentLookupManager) {
        this.persistentLookupManager = persistentLookupManager;
    }

    /**
     * DOC talend AbstractStoreOnDiskHandler constructor comment.
     * 
     * @throws IOException
     */
    protected AbstractStoreOnDiskHandler(String container, int buffSize) throws IOException {
        initPersistentLookupManager(container, buffSize);
    }

    protected abstract void initPersistentLookupManager(String container, int buffSize) throws IOException;

    public void beginQuery() throws Exception {
        persistentLookupManager.initPut();
    }

    public abstract void handleRow(Object[] oneRow) throws Exception;

    public void endQuery() throws Exception {
        persistentLookupManager.endPut();
    }

}
