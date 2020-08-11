package com.DbCompare.org.Hbase.org.ModelData;

import java.util.ArrayList;

public class hbTableRequest {
	
	private String tableName;
	
	private ArrayList<String>  columnFamily;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<String> getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(ArrayList<String> columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Override
	public String toString() {
		return "hbTableRequest [tableName=" + tableName + ", columnFamily=" + columnFamily + "]";
	}
	
	

}
