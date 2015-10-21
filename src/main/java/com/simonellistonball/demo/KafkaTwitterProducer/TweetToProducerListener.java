package com.simonellistonball.demo.KafkaTwitterProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TweetToProducerListener implements StatusListener {
	static final Logger log = Logger.getLogger(TweetToProducerListener.class);
	private String topic;
	private Producer<String, Tweet> producer;

	public TweetToProducerListener(String topic,
			Producer<String, Tweet> producer) {
		super();
		this.topic = topic;
		this.producer = producer;
	}

	public void onStatus(Status status) {
		Tweet tweet = TwitterUtils.parseTweet(status);
		ProducerRecord<String, Tweet> data = new ProducerRecord<String, Tweet>(
				topic, tweet);
		producer.send(data);
	}

	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	}

	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	}

	public void onException(Exception ex) {
		log.error("Error from Twitter: ", ex);
	}

	public void onStallWarning(StallWarning warning) {
		log.warn("Got stall warning:" + warning);
	}

	public void onScrubGeo(long userId, long upToStatusId) {
		log.info("Got scrub_geo event userId:" + userId + " upToStatusId:"
				+ upToStatusId);
	}
}
