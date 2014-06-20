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
package org.talend.dataprofiler.benchmark.MappedByteBuffer.nio;

import java.util.ArrayList;
import java.util.List;

import routines.system.IPersistableComparableLookupRow;
import routines.system.IPersistableLookupRow;

/**
 * created by talend on Jun 9, 2014 Detailled comment
 * 
 */
public abstract class AbstractRow<T> implements IPersistableComparableLookupRow<T>, IPersistableLookupRow<T> {

    private List<String> rowList = new ArrayList<String>();

    private int hashCode = 1;

    // the location of strart from mapping file
    private int startLocation = 0;

    // the length which mapping in the file
    private int length = 1000;

    public boolean hashCodeDirty = true;

    /**
     * Getter for rowList.
     * 
     * @return the rowList
     */
    public List<String> getRowList() {
        return rowList;
    }

    /**
     * Sets the rowList.
     * 
     * @param rowList the rowList to set
     */
    public void setRowList(List<String> rowList) {
        this.rowList = rowList;
    }

    /**
     * Getter for length.
     * 
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * Sets the length.
     * 
     * @param length the length to set
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * Getter for startLocation.
     * 
     * @return the startLocation
     */
    public int getStartLocation() {
        return startLocation;
    }

    /**
     * Sets the startLocation.
     * 
     * @param startLocation the startLocation to set
     */
    public void setStartLocation(int startLocation) {
        this.startLocation = startLocation;
    }

}
