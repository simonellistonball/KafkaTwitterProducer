package com.hortonworks.demo.KafkaTwitterProducer;

import twitter4j.Status;

public class TwitterUtils {

	/**
	 * Map Status object to the fields we're interested in on a Tweet object
	 * 
	 * @param status
	 * @return
	 */
	public static Tweet parseTweet(Status status) {
		Tweet tweet = new Tweet();

		tweet.setDisplayName(cleanString(status.getUser().getScreenName()));
		tweet.setTweet(cleanString(status.getText()));
		tweet.setDate(status.getCreatedAt());
		tweet.setInReplyToScreenName(status.getInReplyToScreenName());
		
		if (status.getGeoLocation() != null){
			tweet.setLongitude(status.getGeoLocation().getLongitude());
			tweet.setLatitude(status.getGeoLocation().getLatitude());
		}
		
		return tweet;
	}

	private static String cleanString(String string) {
		return string.replace("\n", "").replace("\r", "").replace("|","").replace(",","");
	}
}
