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

import org.talend.designer.components.lookup.persistent.AbstractPersistentLookup;
import org.talend.designer.components.lookup.persistent.IPersistentLookupManager;

import routines.system.IPersistableComparableLookupRow;

/**
 * created by talend on Jun 13, 2014 Detailled comment
 * 
 */
public abstract class AbstratcLookupManager<B extends IPersistableComparableLookupRow<B>> extends AbstractPersistentLookup<B>
        implements IPersistentLookupManager<B>, Cloneable {
    // nothing to do
}
