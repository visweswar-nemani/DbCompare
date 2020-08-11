package com.DbCompare.org.MongoDb.MongoController;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.DbCompare.org.Hbase.org.hbaseServiceImpl.hbaseServiceImpl;
import com.DbCompare.org.MongoDb.MongoServiceImpl.IMongoService;
import com.google.common.io.Files;


@Controller
public class MongoController {
	
	private static Logger logger =LogManager.getLogger(MongoController.class);
	
	
	@Autowired
	IMongoService	mongoServiceImpl;
	
	
	@RequestMapping(value="/mongo/insertRecordPage",method=RequestMethod.POST)
	public String insertRecordPage() {
		return "mongo-insertRecord";
	}
	
	
	@RequestMapping(value="/mongo/searchRecordPage",method=RequestMethod.POST)
	public String searchRecordPage() {
		
		return "mongo-searchRecord";
	}
	
		
	@RequestMapping(value="/mongo/createTablePage", method=RequestMethod.POST)
	public String createtablePage() {
		
		return "mongo-createTable";
	}
	
	
	@RequestMapping(value="/mongo/importCSVPage" , method = RequestMethod.POST)
	public String importCSVPage() {
		return "mongo-importCSV";		
	}
	
	
	
	@RequestMapping(value="/mongo/createTable",method=RequestMethod.POST)
	public String createTable(@RequestBody String  request) {
		
		logger.info("   input data {} ",request);
		mongoServiceImpl.createTable(request);
		return "mongo-createTable";
		
	}
	
	@RequestMapping(value="/mongo/insertRecord",method=RequestMethod.POST)
	public String insertRecord(@RequestBody String  request) {
		
		logger.info(" input insert data {} ",request);
		mongoServiceImpl.insertRecord();
		return "mongo-insertRecord";
		
	}
	
	
	@RequestMapping(value="/mongo/searchRecord",method=RequestMethod.POST)
	public String searchRecord(@RequestBody String  request) {
		
		logger.info(" input search data {} ",request);
		mongoServiceImpl.searchRecord();
		return "mongo-searchRecord";		
	}
	
	@RequestMapping(value="/mongo/searchRecordCondition1",method=RequestMethod.POST)
	public String searchRecordCondition1(@RequestBody String  request) {
		
		logger.info(" input search data  with condition {} ",request);
		mongoServiceImpl.searchRecordWithCondition1();
		return "mongo-searchRecord";		
	}
	
	
	@RequestMapping(value="/mongo/searchRecordCondition2",method=RequestMethod.POST)
	public String searchRecordCondition2(@RequestBody String  request) {
		
		logger.info(" input search data  with condition (Query2) -->{} ",request);
		mongoServiceImpl.searchRecordWithCondition2();
		return "mongo-searchRecord";		
	}
	
	
	@RequestMapping(value="/mongo/searchRecord4",method=RequestMethod.POST)
	public String searchRecord4(@RequestBody String  request) {
		
		logger.info(" input search data {} ",request);
		mongoServiceImpl.searchRecord4();
		return "mongo-searchRecord";		
	}
	
	
	@RequestMapping(value="/mongo/searchRecord5",method=RequestMethod.POST)
	public String searchRecord5(@RequestBody String  request) {
		
		logger.info(" input search data {} ",request);
		mongoServiceImpl.searchRecord5();
		return "mongo-searchRecord";		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//Not using
	@RequestMapping(value="/mongo/importCSV123" , method = RequestMethod.POST)
	public String importCSV(@RequestParam("fileCSV") MultipartFile  file) {
		logger.info(" request  received ---> file   {}",file);
		File serverFile=null;
		if(!file.isEmpty()) {
			try {
				byte[] bytes = file.getBytes();
				// Creating the directory to store file
				String rootPath = System.getProperty("catalina.home");
				logger.info("the path is --->{}",rootPath);
				File dir = new File(rootPath + File.separator + "tmpFiles");
				if (!dir.exists()) {
					dir.mkdirs();
				}
				//create file on server 
				serverFile = new File(dir.getAbsolutePath()+ File.separator + file.getOriginalFilename());
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(serverFile));
				stream.write(bytes);
				stream.close();
				logger.info("Server File Location={}  and file name ->{}", serverFile.getAbsolutePath(),file.getOriginalFilename());
			}catch(Exception e) {
				e.printStackTrace();				
			}
		}		
		mongoServiceImpl.importCsv(serverFile);		
		return "mongo-importCSV";		
	}
	
	
	
	@RequestMapping(value="/mongo/importCSV" , method = RequestMethod.POST)
	public String fileUplaod(HttpServletRequest request) {
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		logger.info("The uploaded fileis  of --->{}",isMultipart);
		ServletFileUpload upload = new ServletFileUpload();
		String fileName=null;
		InputStreamReader inputStream = null;
		try {
			FileItemIterator iterStream = upload.getItemIterator(request);
			logger.info(" File item Iterator --->{}  , {}",iterStream,iterStream.hasNext());
			while(iterStream.hasNext()) {
				FileItemStream item = iterStream.next();
				logger.info("FileItemStream   -->{}",item);
				String itemName=item.getFieldName();
				logger.info("itemName  is --->{}",itemName);
				InputStream stream =item.openStream();
				logger.info("InputStream  is --->{}",stream);
				if(!item.isFormField()) {
					fileName=item.getName();
					logger.info("If FormField  then --->{}",fileName);
					inputStream = new InputStreamReader(stream); 
					/*
					 * BufferedReader reader=new BufferedReader(inputStream); //OutputStream out
					 * =new FileOutputStream(fileName); StringBuilder out = new StringBuilder();
					 * String line; int count =0; while((line=reader.readLine())!=null & count<10) {
					 * //out.append(line); logger.info("Out Stream  --->{}",line); count++; }
					 */
					
					//IOUtils.copy(stream,writer);
					//reader.close();
				

					
					//stream.close();
					
					
					logger.info("the uploaded file is --->{}",fileName);
					mongoServiceImpl.importCsv2(inputStream);
				}
			}	
			
		}catch(Exception e ) {
			e.printStackTrace();
			logger.error("The error is at---->{}",e);
		}
		return "mongo-importCSV";
		
	}
	
	
	
	
	
	 
	@RequestMapping(value="/mongo/downloadCSV" , method = RequestMethod.POST)
	public void fileDownload(HttpServletRequest request, HttpServletResponse response) {
		logger.info("the absolute pathis ---->{} and  relative path is -->{}",request.getContextPath(),request.getServletContext().getRealPath("/WEB-INF/downloads/"));
		//Path file = Paths.get(request.getServletContext().getRealPath("/WEB-INF/downloads/"), "reports.csv");
		 File file = new File (request.getServletContext().getRealPath("movies.csv"));
		 try {
			if(file.createNewFile()) {
				logger.info(" file location is  --->{}",file.getAbsolutePath());
				 
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mongoServiceImpl.getAllRecords(file);	
		response.setContentType("application/csv");
		response.addHeader("Content-Disposition", "attachment ; filename"+file.getName());
		try {
			Files.copy(file, response.getOutputStream());
			response.getOutputStream().flush();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		file.delete();
		//return "mongo-importCSV";
		
	}
}
