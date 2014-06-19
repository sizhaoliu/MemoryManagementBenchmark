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
	
	private static final long serialVersionUID = -1898675216005084539L;
	protected List<Object> values = new ArrayList<Object>();
    protected List<Object> keys = new ArrayList<Object>();

    /**
     * Getter for values.
     * 
     * @return the values
     */
    public List<Object> getValue() {
        return this.values;
    }
    
    /**
     * Getter for keys.
     * @return the keys
     */

    public List<Object> getKeys() {
		return keys;
	}



	public DataListRow() {
    }
	
}
