package com.DbCompare.org.MongoDb.MongoDAO;

import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.MongoDb.ModelData.MongoTableResponse;
import com.DbCompare.org.MongoDb.ModelData.Tags;



public interface ImongoDAO {
	
	public MongoTableResponse createTable();
	public MongoTableResponse insertRecord();
	public MongoTableResponse searchRecord(); 
	public MongoTableResponse importCSV(ArrayList<Tags> list);
	public MongoTableResponse searchRecordByCondition();
	public MongoTableResponse searchRecordByCondition2();
	public MongoTableResponse importCSV2(InputStreamReader inputStream);
	public String getAllRecords(File file);
	public MongoTableResponse searchRecord4(); 
	public MongoTableResponse searchRecord5(); 


}
