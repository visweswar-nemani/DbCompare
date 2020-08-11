package com.DbCompare.org.MongoDb.MongoDAO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import com.DbCompare.org.MongoDb.ModelData.Tags;
import org.springframework.stereotype.Repository;
import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.Hbase.org.ModelData.IbaseResponse.status;
import com.DbCompare.org.MongoDb.ModelData.MongoTableResponse;
import com.DbCompare.org.MongoDb.MongoServiceImpl.MongoServiceImpl;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

@Repository
public class MongoDAO implements ImongoDAO {
	
	private static Logger logger =LogManager.getLogger(MongoDAO.class);

	@Override
	public MongoTableResponse createTable() {
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
		MongoTableResponse response = new MongoTableResponse();
		DBCursor cursor=null;
		long startTime=0;
		long endTime = 0;
		MongoCollection<Document> collection =null;
		try {
			MongoClient mongo = new MongoClient("localhost",27017);
			logger.info("Mongo connection address  ---> {}",mongo.getConnectPoint());
			startTime = System.currentTimeMillis();
			MongoDatabase database =  mongo.getDatabase("moviesDataNew");
			collection = database.getCollection("ratingsNew");
			DBObject query = new BasicDBObject("movieId","1449");
			long rows = collection.count(new Document("movieId","1449"));
			endTime = System.currentTimeMillis();
			logger.info("The  record count is -->{}",rows);
			logger.info("time taken for execution --->{}",(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf(endTime-startTime));
		} catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		return response;
	}

	@Override
	public MongoTableResponse importCSV(ArrayList<Tags> tags) {
		MongoTableResponse response = new MongoTableResponse();
		long startTime =0;
		long endTime=0;
		try {
		List<Document> list= new ArrayList<Document>();
		MongoClient mongo = new MongoClient("localhost",27017);
		logger.info("Mongo connection address  ---> {}",mongo.getConnectPoint());
		//connecting to database
		MongoDatabase database = mongo.getDatabase("moviesDataNew");
		//creating a collection 
		MongoCollection<Document> collection =database.getCollection("tags");
		for(int i=0;i<tags.size();i++) {
			list.add(new Document("Id",tags.get(i).getId()).append("userId",tags.get(i).getUserId()).append("movieId",tags.get(i).getMovieId()).append("tagName", tags.get(i).getTagName()));
		}
		startTime=System.currentTimeMillis();
		collection.insertMany(list); 
		endTime = System.currentTimeMillis(); 
		logger.info("time taken to insert documents ----> {}",(endTime-startTime));
		response.setStatus(status.SUCCESS);
		response.setTimeTaken(String.valueOf((endTime-startTime)));
		} catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		// TODO Auto-generated method stub
		return response;
	}

	@Override
	public MongoTableResponse searchRecordByCondition() {
		MongoTableResponse response = new MongoTableResponse();
		DBCursor cursor=null;
		long startTime=0;
		long endTime = 0;
		try {
		MongoClient mongo = new MongoClient("localhost",27017);
		logger.info("Mongo connection address  ---> {}",mongo.getConnectPoint());
		startTime= System.currentTimeMillis();
		DB database= mongo.getDB("moviesDataNew");
		DBCollection collection = database.getCollection("movies");
		DBObject query = new BasicDBObject("titleId","Grumpier Old Men (1995)");
		cursor = collection.find(query);
		endTime = System.currentTimeMillis();
		response.setStatus(status.SUCCESS);
		
			while(cursor.hasNext()) {
				logger.info(cursor.next().toString());
			}
		}catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		} finally {
			cursor.close();
		}
		
		logger.info("time taken to get data -->{}",(endTime-startTime));
		response.setTimeTaken(String.valueOf((endTime-startTime)));
		
		
		// TODO Auto-generated method stub
		return response;
	}

	@Override
	public MongoTableResponse searchRecordByCondition2() {
		// TODO Auto-generated method stub
		MongoTableResponse response = new MongoTableResponse();
		DBCursor cursor=null;
		long startTime=0;
		long endTime = 0;
		try {
		MongoClient mongo = new MongoClient("localhost",27017);
		logger.info("Mongo connection address  ---> {}",mongo.getConnectPoint());
		startTime= System.currentTimeMillis();
		DB database= mongo.getDB("moviesDataNew");
		DBCollection collection = database.getCollection("tags");
		DBObject query = new BasicDBObject("userId","267760").append("movieId","996");		
		cursor = collection.find(query);
		while(cursor.hasNext()) {
			logger.info(cursor.next().toString());
		}
		endTime = System.currentTimeMillis();
		response.setStatus(status.SUCCESS);
		}catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		} finally {
			cursor.close();
		}
		
		logger.info("time taken to get data (Query2)-->{}",(endTime-startTime));
		response.setTimeTaken(String.valueOf((endTime-startTime)));
		return response;
	}

	@Override
	public MongoTableResponse importCSV2(InputStreamReader inputStream) {
		// TODO Auto-generated method stub
		MongoTableResponse response = new MongoTableResponse();
		long startTime=0;
		long endTime = 0;
		BufferedReader reader=null;
		String line=null;
		int counter=0;
		Document  doc = null;
		try {
			reader= new BufferedReader(inputStream);
			
			MongoClient mongo = new MongoClient("localhost",27017);
			logger.info("Mongo connection address  ---> {}",mongo.getConnectPoint());
			//connecting to database
			MongoDatabase database = mongo.getDatabase("moviesDataNew");
			//creating a collection 
			MongoCollection<Document> collection =database.getCollection("movies");
			startTime=System.currentTimeMillis();
			while((line = reader.readLine())!=null) {
				String splitData[]= line.split(",");
				if(counter==1) {
					logger.info("Inserting documents  into Mongo Database --> ");
				}
				doc =new Document("movieId",splitData[0]).append("titleId", splitData[1]).append("genres", splitData[2]);
				collection.insertOne(doc);
				counter++;
			}
			endTime = System.currentTimeMillis(); 
			logger.info("time taken to insert documents in collection {}  is {} milliseconds ----> {}",collection.getNamespace(),(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf((endTime-startTime)));
			inputStream.close();
			reader.close();
			mongo.close();			
		} catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		
		return response;
	}

	@Override
	public String getAllRecords(File file ) {
		// TODO Auto-generated method stub
		int limitVal=50000;
		int skipVal=0;
		long startTime=0;
		long endTime=0;
		FindIterable<Document> docs=null;
		try {
			CSVWriter writer =  new CSVWriter(new FileWriter(file));
			MongoClient mongo =new MongoClient("localhost",27017);
			MongoDatabase database = mongo.getDatabase("moviesDataNew");
			MongoCollection<Document> collection = database.getCollection("ratingsNew");
			startTime=System.currentTimeMillis();
			logger.info("Mongo collection count {}  --> and  name status-->{}",collection.count(), collection.getNamespace());
 pagination:if(true) {
	 			//docs.batchSize(3);
	 			
	 			docs = collection.find(); //.limit(limitVal).skip(skipVal);			
	 			if(!docs.cursor().hasNext()) {
	 				break pagination;
	 			}
	 			for(Document doc : docs) {
	 				
	 				//String line = doc.get("id")+","+doc.get("userId")+","+doc.get("movieId")+","+doc.get("rating")+","+doc.get("ratingtimestamp");
	 				String line[]= {doc.get("id").toString(),doc.get("userId").toString(),doc.get("movieId").toString(),doc.get("rating").toString(),doc.get("ratingtimestamp").toString()};
	 				writer.writeNext(line);
	 				
	 			}
	 			//logger.info("limit and skip values are  {} ,  {}",limitVal,skipVal);
	 			skipVal=skipVal+limitVal;
	 			break pagination;
	 			//limitVal=limitVal+10;
			}
			endTime=System.currentTimeMillis();
			logger.info("time taken to export all documents  from  collection {}  is --->{} milliseconds",collection.getNamespace(),(endTime-startTime));
			writer.close();
			mongo.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		return file.getAbsolutePath();
	}

	@Override
	public MongoTableResponse searchRecord4() {
		// TODO Auto-generated method stub
		MongoTableResponse response = new MongoTableResponse();
		//DBCursor cursor=null;
		long startTime=0;
		long endTime = 0;
		MongoCollection<Document> collection =null;
		try {
			MongoClient mongoClient = new MongoClient("localhost",27017);
			MongoDatabase database = mongoClient.getDatabase("moviesDataNew");
			startTime = System.currentTimeMillis();
			collection= database.getCollection("ratingsNew");
			long rows = collection.count(new Document("movieId","1449").append("rating", new Document("$gte","4.0")));
			endTime = System.currentTimeMillis();
			logger.info("number of records found {} and time taken to find--->{}",rows,(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf(endTime-startTime));
			mongoClient.close();
		}catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		
		return response;
	}

	@Override
	public MongoTableResponse searchRecord5() {
		// TODO Auto-generated method stub
		MongoTableResponse response = new MongoTableResponse();
		//DBCursor cursor=null;
		long startTime=0;
		long endTime = 0;
		MongoCollection<Document> collection =null;
		try {
			MongoClient mongoClient = new MongoClient("localhost",27017);
			MongoDatabase database = mongoClient.getDatabase("moviesDataNew");
			startTime = System.currentTimeMillis();
			collection= database.getCollection("ratingsNew");
			long rows = collection.count(new Document("movieId","1449").append("rating", new Document("$lte","2.0")));
			endTime = System.currentTimeMillis();
			logger.info("number of users  found {} and time taken to find--->{}",rows,(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf(endTime-startTime));
			mongoClient.close();
		}catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		
		return response;
		
	}

}
