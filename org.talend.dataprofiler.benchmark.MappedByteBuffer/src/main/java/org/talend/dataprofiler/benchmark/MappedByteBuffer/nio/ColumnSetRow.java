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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.InvalidMarkException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.logging.Logger;


/**
 * created by talend on Jun 9, 2014 Detailled comment
 * 
 */
public class ColumnSetRow extends AbstractRow<ColumnSetRow> {


    private Long frequency = 1l;

    private MappedByteBuffer mbb = null;

    private FileChannel ch = null;

    public ColumnSetRow() {

    }

    public ColumnSetRow(List<String> rowList) {
        this.setRowList(rowList);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.talend.dq.analysis.persistent.AbstractRow#setRowList(java.util.List)
     */
    @Override
    public void setRowList(List<String> rowList) {
        super.setRowList(rowList);
        // lookup from file and merge frequency
    }

    public void readData() {
        if (mappingFile()) {
            while (mbb.hasRemaining()) {
            	try{
                int strLength = mbb.getInt();
                if (strLength == 0) {
                	break;
                }
                if (strLength == -1) {
                    readValuesData();
                    continue;
                }
                byte[] tempByteArray = new byte[strLength];
                mbb.get(tempByteArray);
                
                    String strReturn = new String(tempByteArray, "UTF-8");
                    this.getRowList().add(strReturn);
                } catch (UnsupportedEncodingException e) {
                }catch(BufferUnderflowException e){
            		break;
            	}
            }
        }
    }

    public void writeData() {
        if (mappingFile()) {
            for (String key : getRowList()) {
                byte[] contents = key.getBytes();
                mbb.putInt(contents.length);
                mbb.put(contents);
            }
            mbb.putInt(-1);
            mbb.putLong(this.frequency);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#writeKeysData(java.io.ObjectOutputStream)
     */
    public void writeKeysData(ObjectOutputStream out) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#readKeysData(java.io.ObjectInputStream)
     */
    public void readKeysData(ObjectInputStream in) {
        if (mappingFile()) {
            while (mbb.hasRemaining()) {
                int strLength = mbb.getInt();
                if (strLength == -1) {
                    readValuesData();
                    continue;
                }
                byte[] tempByteArray = new byte[strLength];
                mbb.get(tempByteArray);
                try {
                    String strReturn = new String(tempByteArray, "UTF-8");
                    this.getRowList().add(strReturn);
                } catch (UnsupportedEncodingException e) {
                }
            }
        }

    }

    /**
     * DOC talend Comment method "readValuesData".
     */
    private void readValuesData() {
        this.frequency = mbb.getLong();
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#writeValuesData(java.io.DataOutputStream, java.io.ObjectOutputStream)
     */
    public void writeValuesData(DataOutputStream dataOut, ObjectOutputStream objectOut) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#readValuesData(java.io.DataInputStream, java.io.ObjectInputStream)
     */
    public void readValuesData(DataInputStream dataIn, ObjectInputStream objectIn) {
        if (mappingFile()) {
            while (mbb.hasRemaining()) {
                int strLength = mbb.getInt();
                if (strLength != -1) {
                    continue;
                } else {
                    readValuesData();
                }
            }
        }

    }

    /**
     * Getter for frequency.
     * 
     * @return the frequency
     */
    public Long getFrequency() {
        return frequency;
    }

    /**
     * Sets the frequency.
     * 
     * @param frequency the frequency to set
     */
    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#copyDataTo(java.lang.Object)
     */
    public void copyDataTo(ColumnSetRow other) {
        other.setFrequency(this.getFrequency());
        other.setMbb(this.getMbb());
        other.setCh(this.getCh());
        other.setStartLocation(this.getStartLocation());
        other.setLength(this.getLength());
    }

    public void increaseFrequency() {
        this.frequency++;
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.IPersistableLookupRow#copyKeysDataTo(java.lang.Object)
     */
    public void copyKeysDataTo(ColumnSetRow other) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(ColumnSetRow o) {
        if (o == null) {
            return 1;
        }
        if (this.getRowList() != null && o.getRowList() != null) {
            if (this.getRowList().equals(o.getRowList())) {
                return 0;
            }
        }

        if (this.getRowList() == null && o.getRowList() == null) {
            return 0;
        }
        return 1;
    }

    /**
     * Sets the ch.
     * 
     * @param ch the ch to set
     */
    public void setCh(FileChannel ch) {
        this.ch = ch;
    }

    /**
     * Getter for ch.
     * 
     * @return the ch
     */
    public FileChannel getCh() {
        return this.ch;
    }

    /**
     * Getter for mbb.
     * 
     * @return the mbb
     */
    public MappedByteBuffer getMbb() {
        return this.mbb;
    }

    /**
     * Sets the mbb.
     * 
     * @param mbb the mbb to set
     */
    public void setMbb(MappedByteBuffer mbb) {
        this.mbb = mbb;
    }

    private boolean needMappingData() {
        if (ch == null) {
            return false;
        }
        if (mbb == null) {
            return true;
        } else {
            try {
                mbb.reset();
            } catch (InvalidMarkException e) {
                return true;
            }
        }
        return false;
    }

    private boolean mappingFile() {
        if (needMappingData()) {
            try {
                mbb = ch.map(MapMode.READ_WRITE, getStartLocation(), getLength());
                mbb.mark();
            } catch (IOException e) {
                return false;
            }
        }
        return true;
    }

}
