package com.hortonworks.demo.KafkaTwitterProducer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

public class TweetSerializer implements Serializer<Tweet> {

	private Gson gson;

	public void configure(Map<String, ?> configs, boolean isKey) {
		gson = new Gson();
	}

	public byte[] serialize(String topic, Tweet data) {
		return gson.toJson(data).getBytes();
	}

	public void close() {
		gson = null;
	}
}
