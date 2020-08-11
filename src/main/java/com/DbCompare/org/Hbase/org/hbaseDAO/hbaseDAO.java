package com.DbCompare.org.Hbase.org.hbaseDAO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import com.DbCompare.org.Hbase.org.ModelData.IbaseResponse.status;
import com.DbCompare.org.Hbase.org.ModelData.Movies;
import com.DbCompare.org.Hbase.org.ModelData.Ratings;
import com.DbCompare.org.Hbase.org.ModelData.Tag;
import com.DbCompare.org.Hbase.org.ModelData.hbTableRequest;
import com.DbCompare.org.Hbase.org.ModelData.hbTableResponse;
import com.DbCompare.org.MongoDb.ModelData.Tags;
import com.opencsv.CSVWriter;


@Repository
public class hbaseDAO implements IhbaseDAO {
	
	
	private static final byte[] POSTFIX = new byte [] { 0x000000 };
	
	private static  Logger logger=LogManager.getLogger(hbaseDAO.class);

	public Admin getAdminAccess() {
		
		Admin admin = null;
		
		logger.info("getting admin access");
	    try {
	    	Configuration config = HBaseConfiguration.create();
		
		
			Connection connection = ConnectionFactory.createConnection(config);
			
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
	    
	    return admin;		
	}
	
	
	@Override
	public hbTableResponse createTable(hbTableRequest request) {
		
		hbTableResponse response=new hbTableResponse();
		
		Admin admin= getAdminAccess();
		HTableDescriptor table =  new HTableDescriptor(TableName.valueOf(request.getTableName()));
		if(request.getColumnFamily().size()>0) {
			
			for(String columnFamily:request.getColumnFamily()) {
				table.addFamily(new HColumnDescriptor(columnFamily));
			}
		}
			
		try {
			if(admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
		long startTime = System.currentTimeMillis();
		admin.createTable(table);
		long endTime = System.currentTimeMillis();
		logger.debug(" time taken in Hbase {}",(endTime-startTime));
		logger.info("table created");
		response.setStatus(status.SUCCESS);
		response.setTimeTaken(String.valueOf((endTime-startTime)));
		logger.info("response after db execution ---> {}",response);
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			response.setStatus(status.FAILURE);
		}
		// TODO Auto-generated method stub
		return response;
	}
	
	
	
	
	
	public hbTableResponse insertRecord() {
		
		hbTableResponse response= new hbTableResponse();
		/*
		 * if(request ==null) {
		 * logger.debug(" request received in DAO insertRecord -->{}",request);
		 * response.setStatus(status.FAILURE); return response; }
		 */
		
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		
		
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime=System.currentTimeMillis();
		
			table=connection.getTable(TableName.valueOf("test123"));
		
		
			String testData[][]= {{"1","Jack","Ryan","45","USA","jack@yopmail.com"},
							  {"2","John","peter","54","Japan","peter@yopmail.com"},
							  {"3","Harry","potter","32","Canada","harry@yopmail.com"}};
		
			
			for(int i=0;i<testData.length;i++) {
				Put people = new Put(Bytes.toBytes(testData[i][0]));
				people.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("fname"), Bytes.toBytes(testData[i][1]));
				people.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("1name"), Bytes.toBytes(testData[i][2]));
				people.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("age"), Bytes.toBytes(testData[i][3]));
				people.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("country"), Bytes.toBytes(testData[i][4]));
				people.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("email"), Bytes.toBytes(testData[i][5]));
				table.put(people);
			}
			long endTime=System.currentTimeMillis();
			logger.info("start time --->{}",startTime);
			logger.info("end time --->{}",endTime);
			logger.info("time taken for insertion of data -->{}",(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf((endTime-startTime)));
		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				response.setStatus(status.FAILURE);
		}
		
		return response;
	}


	@Override
	public hbTableResponse searchRecord() {
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		Filter filter = null;
		Scan scan= null;
		int count = 0;
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime = System.currentTimeMillis();
			table=connection.getTable(TableName.valueOf("ratingsNew"));
			filter = new SingleColumnValueFilter(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId"), CompareOperator.EQUAL, Bytes.toBytes("1449"));
			scan = new Scan();
			scan.setFilter(filter);
			//Get g = new Get(Bytes.toBytes("1"));
			ResultScanner r = table.getScanner(scan);
			
			for(Result res : r) {
				//logger.info(Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("userId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("rating")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("ratingTimestamp"))));
				count++;
			}
			long endTime = System.currentTimeMillis();
			//String valStr=Bytes.toString(val);
			logger.info("no of ratings given for movieID {}   ---> {}",filter.toString(),count);
			logger.info("time taken for getting data -->{}",(endTime-startTime));
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
	public hbTableResponse importCSV(ArrayList<Tags> list) {
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		logger.info("import data size  {}",list.size());
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime=System.currentTimeMillis();		
			table=connection.getTable(TableName.valueOf("tags"));
			logger.debug("connection status ---- >{}",connection);
			for(int i=0;i<list.size();i++) {
				Put tagData= new Put(Bytes.toBytes(list.get(i).getId()));
				//tagData.addColumn(Bytes.toBytes("tagData"), Bytes.toBytes("Id"),Bytes.toBytes(list.get(i).getId()));
				tagData.addColumn(Bytes.toBytes("tagData"), Bytes.toBytes("userId"),Bytes.toBytes(list.get(i).getUserId()));
				tagData.addColumn(Bytes.toBytes("tagData"), Bytes.toBytes("movieId"),Bytes.toBytes(list.get(i).getMovieId()));
				tagData.addColumn(Bytes.toBytes("tagData"), Bytes.toBytes("tagName"),Bytes.toBytes(list.get(i).getTagName()));
				table.put(tagData);
			}
			long endTime=System.currentTimeMillis();
			logger.debug("start time --->{}",startTime);
			logger.debug("end time --->{}",endTime);
			logger.info("time taken for importing  data through CSV -->{}",(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf((endTime-startTime)));
		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				response.setStatus(status.FAILURE);
		}
		
		// TODO Auto-generated method stub
		return response;
	}
	
	
	
	@Override
	public hbTableResponse searchRecordByCondition() {
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Filter filter=null;
		Table table=null;
		Scan s=null;
		ResultScanner result;
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime = System.currentTimeMillis();
			table=connection.getTable(TableName.valueOf("movies"));
			filter = new SingleColumnValueFilter(Bytes.toBytes("Data"),Bytes.toBytes("titleId"),CompareOperator.EQUAL,Bytes.toBytes("Grumpier Old Men (1995)"));
			//Get g = new Get(Bytes.toBytes("1"));
			s= new Scan();
			s.setFilter(filter);
			//Result r = table.get(g);
			result = table.getScanner(s);
			for(Result r : result) {
				logger.info(Bytes.toString(r.getValue(Bytes.toBytes("Data"),Bytes.toBytes("titleId")))+ "    "+Bytes.toString(r.getValue(Bytes.toBytes("Data"),Bytes.toBytes("genres"))));
			}
			long endTime = System.currentTimeMillis();
			//byte [] val= r.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("email"));
			//String valStr=Bytes.toString(val);
			//logger.info("info of get ---> {}",valStr);
			logger.info("time taken for finding movie record -->{}",(endTime-startTime));
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
	public hbTableResponse searchRecordByCondition2() {
		// TODO Auto-generated method stub
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		FilterList filterList;
		Filter filter=null;
		Filter filter2=null;
		Table table=null;
		Scan s=null;
		ResultScanner result;
		try {
			connection=ConnectionFactory.createConnection(config);
			filterList = new FilterList(Operator.MUST_PASS_ALL);
			filter = new SingleColumnValueFilter(Bytes.toBytes("tagData"),Bytes.toBytes("userId"),CompareOperator.EQUAL,Bytes.toBytes("267760"));
			filter2 = new SingleColumnValueFilter(Bytes.toBytes("tagData"),Bytes.toBytes("movieId"),CompareOperator.EQUAL,Bytes.toBytes("996"));
			filterList.addFilter(filter);
			filterList.addFilter(filter2);
			long startTime = System.currentTimeMillis();
			table=connection.getTable(TableName.valueOf("tags"));
			
			//Get g = new Get(Bytes.toBytes("1"));
			s= new Scan();
			s.setFilter(filterList);
			//Result r = table.get(g);
			result = table.getScanner(s);
			for(Result r : result) {
				logger.info(Bytes.toString(r.getValue(Bytes.toBytes("tagData"),Bytes.toBytes("userId")))+ "  "+Bytes.toString(r.getValue(Bytes.toBytes("tagData"),Bytes.toBytes("movieId"))) +"    "+Bytes.toString(r.getValue(Bytes.toBytes("tagData"),Bytes.toBytes("tagName"))));
			}
			long endTime = System.currentTimeMillis();
			//byte [] val= r.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("email"));
			//String valStr=Bytes.toString(val);
			//logger.info("info of get ---> {}",valStr);
			logger.info("time taken  for searchRecordByCondition2 -->{}",(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf((endTime-startTime)));
			
		} catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);		
		}
		
		

		return null;
	}


	@Override
	public hbTableResponse importCSV2(InputStreamReader stream) {
		// TODO Auto-generated method stub
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;		
		BufferedReader reader = new BufferedReader(stream);
		String line=null;
		int counter=0;
		long startTime=0;
		long endTime=0;
		logger.info(" stream read by buffered reader-->{}",reader);		
		  try { 
			  connection = ConnectionFactory.createConnection(config);
			  startTime=System.currentTimeMillis();
			  table = connection.getTable(TableName.valueOf("tags"));
			  logger.debug(" connection status is  -->{}",connection);
			  while((line=reader.readLine())!=null ) {
				  //logger.info("line --->{}",line); 
				  String splitData[] = line.split(",");
				  //logger.info(" the data in each line--->{}, {} ",splitData[0] instanceof String, splitData[0]);
				  Put tagData = new Put(Bytes.toBytes(splitData[0]));
				  //logger.info(" the data in each line--->{}",Integer.parseInt(splitData[0]));
				  tagData.addColumn(Bytes.toBytes("tagData"),Bytes.toBytes("userId") ,Bytes.toBytes(splitData[1]));
				  tagData.addColumn(Bytes.toBytes("tagData"),Bytes.toBytes("movieId") ,Bytes.toBytes(splitData[2]));
				  tagData.addColumn(Bytes.toBytes("tagData"),Bytes.toBytes("tagName") ,Bytes.toBytes(splitData[3]));
				  tagData.addColumn(Bytes.toBytes("tagData"),Bytes.toBytes("tagTimestamp") ,Bytes.toBytes(splitData[4]));
				  table.put(tagData);
				  
				  //counter++;
				  }
			  endTime=System.currentTimeMillis();
			  logger.info(" ttime taken to import {}   Table--->{} milliseconds",table.getName(),(endTime-startTime));
			  response.setStatus(status.SUCCESS);
			  response.setTimeTaken(String.valueOf((endTime-startTime)));
			  } catch (IOException e) {
				  // TODO Auto-generated
				  e.printStackTrace();
				  response.setStatus(status.FAILURE);
			  }
		return response;
	}


	@Override
	public String getAllRecords(File file) {
		// TODO Auto-generated method stub
		
		long startTime=0;
		long endTime=0;
		Configuration config=null;
		Connection connection=null;
		Table table=null;
		Scan s= null;
		Filter filter=null;
		byte[] lastRow = null;
		byte[] startRow=null;
		ResultScanner result =null;
		int count=0;
		String offset="0";
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			config= HBaseConfiguration.create();
			filter=new PageFilter(100000);
			connection=ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("ratingsNew"));
			logger.debug(" connection status is  -->{}",connection);
			String title[]= {"Id,userId,movieId","rating","ratingtimestamp"};
			writer.writeNext(title);
			startTime=System.currentTimeMillis();
 pagination:while(true) {
	 			s= new Scan();
	 			s.setMaxResultSize(-1);
				/*
				 * if(lastRow!=null) { //startRow = Bytes.add(lastRow,POSTFIX);
				 * //logger.info("start row from last row -->{}", Bytes.toString(startRow));
				 * s.setStartRow(lastRow); }
				 */			
				result = table.getScanner(s);
				//logger.debug("iteration is {} and vaue is {}",count,result.next().getRow());
				if(result.next()==null) {
					logger.info("export ended - last  {} and start rows-->{}",Bytes.toString(lastRow),Bytes.toString(startRow));
					endTime=System.currentTimeMillis();
					break pagination;
				}
			
				for(Result r : result) {
					  if(r==null) { 
						  logger.info("No data found further  --->{}",r.current());
						  endTime=System.currentTimeMillis();
						  break pagination;
					  }						 
					String line[]= {Bytes.toString(r.getRow()),Bytes.toString(r.getValue(Bytes.toBytes("ratingData"),Bytes.toBytes("userId"))),Bytes.toString(r.getValue(Bytes.toBytes("ratingData"),Bytes.toBytes("movieId"))),Bytes.toString(r.getValue(Bytes.toBytes("ratingData"),Bytes.toBytes("rating"))),Bytes.toString(r.getValue(Bytes.toBytes("ratingData"),Bytes.toBytes("ratingTimestamp")))};
					writer.writeNext(line);
					lastRow=r.getRow();	
					if(lastRow==null) {
		 				logger.info("last row null  --->count is: {}  and value is {}",count,Bytes.toString(lastRow));
		 				break pagination;
		 			}
				
				}
				
				endTime=System.currentTimeMillis();
				break pagination;
				/*
				 * if(count%40 == 0) {
				 * logger.info("round completed-->{} and the last row is {}",count,Bytes.
				 * toString(lastRow)); }
				 */
				//count++;				
			}
			
			logger.info("time taken to export data from {} table is -->{} milliseconds",table.getName(),(String.valueOf(endTime-startTime)));
			writer.close();
			connection.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return file.getAbsolutePath();
		
	}


	@Override
	public hbTableResponse searchRecord4() {
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		Filter filter1 = null;
		Filter filter2 = null;
		FilterList filterList=null;
		Scan scan= null;
		int count = 0;
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime = System.currentTimeMillis();
			logger.info("connection status is  --->{}",connection.toString());
			table=connection.getTable(TableName.valueOf("ratingsNew"));
			filter1 = new SingleColumnValueFilter(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId"), CompareOperator.EQUAL, Bytes.toBytes("1449"));
			filter2 = new SingleColumnValueFilter(Bytes.toBytes("ratingData"), Bytes.toBytes("rating"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("4.0"));
			filterList = new FilterList(Operator.MUST_PASS_ALL);
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);
			scan = new Scan();
			scan.setFilter(filterList);
			
			//Get g = new Get(Bytes.toBytes("1"));
			ResultScanner r = table.getScanner(scan);
			
			for(Result res : r) {
				if(count < 25) {
					logger.info(Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("userId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("rating")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("ratingTimestamp"))));
				}				
				count++;
			}
			long endTime = System.currentTimeMillis();
			//String valStr=Bytes.toString(val);
			logger.info("no of users  given rating for movieID {} ,{}  ---> {}",filter1.toString(),filter2.toString(),count);
			logger.info("time taken for getting data -->{}",(endTime-startTime));
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
	public hbTableResponse searchRecord5() {
		hbTableResponse response= new hbTableResponse();
		Configuration config=HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		Filter filter1 = null;
		Filter filter2 = null;
		FilterList filterList=null;
		Scan scan= null;
		int count = 0;
		try {
			connection=ConnectionFactory.createConnection(config);
			long startTime = System.currentTimeMillis();
			logger.info("connection status is  --->{}",connection.toString());
			table=connection.getTable(TableName.valueOf("ratingsNew"));
			filter1 = new SingleColumnValueFilter(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId"), CompareOperator.EQUAL, Bytes.toBytes("1449"));
			filter2 = new SingleColumnValueFilter(Bytes.toBytes("ratingData"), Bytes.toBytes("rating"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes("2.0"));
			filterList = new FilterList(Operator.MUST_PASS_ALL);
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);
			scan = new Scan();
			scan.setFilter(filterList);
			
			//Get g = new Get(Bytes.toBytes("1"));
			ResultScanner r = table.getScanner(scan);
			
			for(Result res : r) {
				if(count < 25) {
					logger.info(Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("userId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("rating")))+"   "+Bytes.toString(res.getValue(Bytes.toBytes("ratingData"), Bytes.toBytes("ratingTimestamp"))));
				}				
				count++;
			}
			long endTime = System.currentTimeMillis();
			//String valStr=Bytes.toString(val);
			logger.info("no of ratings  given by user  {} ,{}  ---> {}",filter1.toString(),filter2.toString(),count);
			logger.info("time taken for getting data -->{}",(endTime-startTime));
			response.setStatus(status.SUCCESS);
			response.setTimeTaken(String.valueOf((endTime-startTime)));
			
		} catch(Exception e) {
			e.printStackTrace();
			response.setStatus(status.FAILURE);		
		}
			
		// TODO Auto-generated method stub
		return response;
	}

}
