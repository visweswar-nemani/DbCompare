package com.DbCompare.org.Hbase.org.ModelData;

public class hbTableResponse implements IbaseResponse{
	
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
	public void setTimeTaken(String timeTakento) {
		this.timeTaken = timeTakento;
	}
	
	
	@Override
	public String toString() {
		return "hbTableResponse [status=" + status + ", timeTaken=" + timeTaken + "]";
	}
	
	
	

}
