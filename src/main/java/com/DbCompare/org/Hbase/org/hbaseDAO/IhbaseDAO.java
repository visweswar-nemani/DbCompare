package com.DbCompare.org.Hbase.org.hbaseDAO;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Admin;

import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.Hbase.org.ModelData.Ratings;
import com.DbCompare.org.Hbase.org.ModelData.Tag;
import com.DbCompare.org.Hbase.org.ModelData.hbTableRequest;
import com.DbCompare.org.Hbase.org.ModelData.hbTableResponse;
import com.DbCompare.org.MongoDb.ModelData.Tags;

public interface IhbaseDAO {
	
	
	
	public hbTableResponse createTable(hbTableRequest request);
	public hbTableResponse insertRecord();
	public hbTableResponse searchRecord(); 
	public hbTableResponse importCSV(ArrayList<Tags> list);
	public hbTableResponse searchRecordByCondition();
	public hbTableResponse searchRecordByCondition2();
	public hbTableResponse importCSV2(InputStreamReader	 stream);
	public String getAllRecords(File file);
	public hbTableResponse searchRecord4(); 
	public hbTableResponse searchRecord5(); 
	

}
