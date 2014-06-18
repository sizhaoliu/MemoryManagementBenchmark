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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * created by talend on Jun 17, 2014 Detailled comment
 * 
 */
public class DataListRow implements Serializable {

    protected List<Object> data = new ArrayList<Object>();

    /**
     * Getter for data.
     * 
     * @return the data
     */
    public List<Object> getData() {
        return this.data;
    }

    public DataListRow(List<Object> data) {
        this.data.addAll(data);
    }

    public DataListRow(Object[] dataArrays) {
        for (Object data : dataArrays) {
            this.data.add(data);
        }
    }

    public DataListRow(Object data) {
        this.data.add(data);
    }
}
