package com.DbCompare.org.Hbase.org.hbaseServiceImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.DbCompare.org.Hbase.org.ModelData.IbaseResponse.status;
import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.Hbase.org.ModelData.Ratings;
import com.DbCompare.org.Hbase.org.ModelData.Tag;
import com.DbCompare.org.Hbase.org.ModelData.hbTableRequest;
import com.DbCompare.org.Hbase.org.ModelData.hbTableResponse;
import com.DbCompare.org.Hbase.org.hbaseDAO.*;
import com.DbCompare.org.MongoDb.ModelData.Tags;

@Component
public class hbaseServiceImpl implements Ihbase {
	
	
	
	@Autowired
	IhbaseDAO hbaseDAO;
	
	
	public IhbaseDAO gethbaseDAO() {
		logger.info("in getter of hbase DAO");
		return hbaseDAO;
	}
	
	public void setHbaseDAO(hbaseDAO hbaseDAO) {
		logger.info("in setter of hbaseDAO");
		this.hbaseDAO=hbaseDAO;
	}
	
	
	public hbTableRequest convertDatatoRequest(String data) {
		
		HashMap<String,String>  keyVal= new HashMap<String,String> ();
		hbTableRequest hTableData = new hbTableRequest();
		
		String splitData[]= data.split("&");
		for(String input: splitData) {
			String innerData[]= input.split("=");
			for(String innerElement: innerData) {
				keyVal.put(innerData[0], innerData[1]);	
				
				logger.debug("data after splitting --> {}", keyVal);				
			}			
		}
		
		hTableData.setTableName(keyVal.get("tableName"));
		keyVal.remove("tableName");
		hTableData.setColumnFamily( new ArrayList<String>(keyVal.values()));
		
		return hTableData;		
	}
	
	
	
	private static Logger logger =LogManager.getLogger(hbaseServiceImpl.class);

	@Override
	public hbTableResponse createTable(String data) {
		hbTableResponse response;
		hbTableRequest request;
		if(data==null) {
			return null;
		}
		request= new hbTableRequest();
		request=convertDatatoRequest(data);
		logger.info("data after converting to request ------->  {}",request);
		response=hbaseDAO.createTable(request); // TODO Auto-generated method stub
		logger.info("{}",response);		 
		return response;
	}

	@Override
	public hbTableResponse insertRecord() {
		// TODO Auto-generated method stub
		//hbTableRequest request=new hbTableRequest();
		logger.info("sending record data to DAO --->{}");
		return hbaseDAO.insertRecord();
	}

	@Override
	public hbTableResponse searchRecord() {
		// TODO Auto-generated method stub
		logger.info("sending retrieval data to DAO --->{}");
		
		return hbaseDAO.searchRecord();
	}
	
	@Override
	public hbTableResponse searchRecordWithCondition1() {
		// TODO Auto-generated method stub
		logger.info("sending retrieval data to DAO --->{}");
		
		return hbaseDAO.searchRecordByCondition();
	}

	@Override
	public hbTableResponse importCsv(File requestFile) {
		hbTableResponse response = new hbTableResponse();
		logger.info("the file --->{}",requestFile.getName());
		String line="";
		String delimitter=",";
		BufferedReader reader=null;
		Tags tags=null;
		ArrayList<Tags> list= new ArrayList<>();
		try {
			reader = new BufferedReader(new FileReader(requestFile));
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
			
		}catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);			
		}
		
		// TODO Auto-generated method stub
		return hbaseDAO.importCSV(list);
	}

	@Override
	public hbTableResponse searchRecordWithCondition2() {
		// TODO Auto-generated method stub
		logger.info("sending retrieval(query2) data to DAO --->{}");
		
		return hbaseDAO.searchRecordByCondition2();
	}

	@Override
	public hbTableResponse importCsv2(InputStreamReader stream) {
		// TODO Auto-generated method stub
		hbTableResponse response = new hbTableResponse();
		logger.info("the file reached serviceImpl --->");
		return hbaseDAO.importCSV2(stream);
	}

	@Override
	public String getAllRecords(File file) {
		// TODO Auto-generated method stub
		return hbaseDAO.getAllRecords(file);
		
	}

	@Override
	public hbTableResponse searchRecord4() {
		// TODO Auto-generated method stub
		return hbaseDAO.searchRecord4();
	}

	@Override
	public hbTableResponse searchRecord5() {
		// TODO Auto-generated method stub
		return hbaseDAO.searchRecord5();
	}


	

	
	
	



}
