package org.talend.dataprofiler.benchmark.MapDB.test;

public class TempTuple {
	public TempTuple(String key,Long value){
		this.key=key;
		this.value=value;
	}
	private String key;
	private Long value;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Long getValue() {
		return value;
	}
	public void setValue(Long value) {
		this.value = value;
	}
	

}
