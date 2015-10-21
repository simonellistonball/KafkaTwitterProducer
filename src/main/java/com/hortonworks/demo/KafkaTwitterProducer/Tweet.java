package com.hortonworks.demo.KafkaTwitterProducer;

import java.util.Date;

/**
 * Domain Object to represent tweet
 * 
 * This is what we will serialise onto Kafka
 * 
 * @author sball
 *
 */
public class Tweet {
	
	private String displayName;
	private String tweet;
	private Date date;
	private String inReplyToScreenName;
	private double longitude;
	private double latitude;
	
	
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getTweet() {
		return tweet;
	}
	public void setTweet(String tweet) {
		this.tweet = tweet;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getInReplyToScreenName() {
		return inReplyToScreenName;
	}
	public void setInReplyToScreenName(String inReplyToScreenName) {
		this.inReplyToScreenName = inReplyToScreenName;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
}
