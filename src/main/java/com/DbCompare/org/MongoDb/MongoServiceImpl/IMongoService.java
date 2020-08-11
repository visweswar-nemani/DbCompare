package com.DbCompare.org.MongoDb.MongoServiceImpl;

import java.io.File;
import java.io.InputStreamReader;

import com.DbCompare.org.MongoDb.ModelData.MongoTableResponse;

public interface IMongoService {
	
	public MongoTableResponse createTable(String request);
	
	public MongoTableResponse insertRecord();
	
	public MongoTableResponse searchRecord();
	
	public MongoTableResponse importCsv(File file);
	
	public MongoTableResponse searchRecordWithCondition1();
	
	public MongoTableResponse searchRecordWithCondition2();

	public MongoTableResponse importCsv2(InputStreamReader inputStream);

	public String getAllRecords(File file);
	
	public MongoTableResponse searchRecord4();
	
	public MongoTableResponse searchRecord5();


}
