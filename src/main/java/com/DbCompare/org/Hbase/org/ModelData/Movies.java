package com.DbCompare.org.Hbase.org.ModelData;

public class Movies {
	private String movieId;
	private String title;
	private String genres;
	public String getMovieId() {
		return movieId;
	}
	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getGenres() {
		return genres;
	}
	public void setGenres(String genres) {
		this.genres = genres;
	}
	@Override
	public String toString() {
		return "Movies [movieId=" + movieId + ", title=" + title + ", genres=" + genres + "]";
	}
	
	
	
	
}
