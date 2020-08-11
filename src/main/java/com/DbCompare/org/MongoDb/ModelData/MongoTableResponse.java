package com.DbCompare.org.MongoDb.ModelData;

import com.DbCompare.org.Hbase.org.ModelData.IbaseResponse.status;

public class MongoTableResponse {
	
	private status status;
	private String timeTaken;
	public status getStatus() {
		return status;
	}
	public void setStatus(status status) {
		this.status = status;
	}
	public String getTimeTaken() {
		return timeTaken;
	}
	public void setTimeTaken(String timeTaken) {
		this.timeTaken = timeTaken;
	}
	@Override
	public String toString() {
		return "MongoTableResponse [status=" + status + ", timeTaken=" + timeTaken + "]";
	}
	
	
	
	
}
