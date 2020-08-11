package com.DbCompare.org.MongoDb.MongoServiceImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.Hbase.org.ModelData.Ratings;
import com.DbCompare.org.Hbase.org.ModelData.hbTableResponse;
import com.DbCompare.org.Hbase.org.hbaseDAO.hbaseDAO;
import com.DbCompare.org.MongoDb.ModelData.MongoTableResponse;
import com.DbCompare.org.MongoDb.ModelData.Tags;
import com.DbCompare.org.MongoDb.MongoDAO.ImongoDAO;
import com.DbCompare.org.MongoDb.MongoDAO.MongoDAO;



@Component
public class MongoServiceImpl implements IMongoService{
	
	
	
	private static Logger logger =LogManager.getLogger(MongoServiceImpl.class);
	
		
	@Autowired
	ImongoDAO mongoDAO;
	
	

	@Override
	public MongoTableResponse createTable(String request) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MongoTableResponse insertRecord() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MongoTableResponse searchRecord() {
		// TODO Auto-generated method stub
		
		return mongoDAO.searchRecord();
	}

	@Override
	public MongoTableResponse importCsv(File file) {
		
		logger.info("the file --->{}",file.getName());
		String line="";
		String delimitter=",";
		BufferedReader reader=null;
		Tags tags=null;
		ArrayList<Tags> list= new ArrayList<>();
		try {
			reader = new BufferedReader(new FileReader(file));
			while((line=reader.readLine())!=null) {
				tags= new Tags();
				String data [] = line.split(delimitter);
				tags.setId(data[0]);
				tags.setUserId(data[1]);
				tags.setMovieId(data[2]);	
				tags.setTagName(data[3]);
				list.add(tags);
			}
			logger.info("data in the file --> {}",list.size());	
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return mongoDAO.importCSV(list);
	}

	@Override
	public MongoTableResponse searchRecordWithCondition1() {
		// TODO Auto-generated method stub
		return mongoDAO.searchRecordByCondition();
	}

	@Override
	public MongoTableResponse searchRecordWithCondition2() {
		// TODO Auto-generated method stub
		
		return mongoDAO.searchRecordByCondition2();
	}

	@Override
	public MongoTableResponse importCsv2(InputStreamReader inputStream) {
		// TODO Auto-generated method stub
		MongoTableResponse response = new MongoTableResponse();
		logger.info("the file reached Mongo serviceImpl --->");
		return mongoDAO.importCSV2(inputStream);		
	}

	@Override
	public String getAllRecords(File file) {
		// TODO Auto-generated method stub
		return mongoDAO.getAllRecords(file);
		
	}

	@Override
	public MongoTableResponse searchRecord4() {
		// TODO Auto-generated method stub
		return mongoDAO.searchRecord4();
	}

	@Override
	public MongoTableResponse searchRecord5() {
		// TODO Auto-generated method stub
		return mongoDAO.searchRecord5();
	}
	
	
	
	
	

}
