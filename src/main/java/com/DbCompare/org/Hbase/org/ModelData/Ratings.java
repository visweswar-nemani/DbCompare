package com.DbCompare.org.Hbase.org.ModelData;

public class Ratings {
	private String id;
	private String userId;
	private String movieId;
	private String rating;
	private String timestamp;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getMovieId() {
		return movieId;
	}
	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}
	public String getRating() {
		return rating;
	}
	public void setRating(String rating) {
		this.rating = rating;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	@Override
	public String toString() {
		return "Ratings [id=" + id + ", userId=" + userId + ", movieId=" + movieId + ", rating=" + rating
				+ ", timestamp=" + timestamp + "]";
	}
	
	
	
}
