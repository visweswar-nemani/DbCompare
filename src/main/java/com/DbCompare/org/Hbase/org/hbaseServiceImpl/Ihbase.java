package com.DbCompare.org.Hbase.org.hbaseServiceImpl;


import java.io.File;
import java.io.InputStreamReader;

import com.DbCompare.org.Hbase.org.ModelData.hbTableResponse;

public interface Ihbase {
	
	public hbTableResponse createTable(String request);
	
	public hbTableResponse insertRecord();
	
	public hbTableResponse searchRecord();
	
	public hbTableResponse importCsv(File file);
	
	public hbTableResponse searchRecordWithCondition1();
	
	public hbTableResponse searchRecordWithCondition2();
	
	public hbTableResponse importCsv2(InputStreamReader reader);

	public String getAllRecords(File file);
	
	public hbTableResponse searchRecord4();
	
	public hbTableResponse searchRecord5();
	

}
