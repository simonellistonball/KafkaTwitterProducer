package com.simonellistonball.demo.KafkaTwitterProducer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Hello world!
 *
 */
public class App {
	static final Logger log = Logger.getLogger(App.class);
	private static final String TWITTER4J_PROPERTIES = "twitter4j.properties";
	private static final String DEFAULT_TOPIC = "tweets";

	public static void main(String[] args) {
		CommandLine cmd;
		String brokers = null;
		String[] hashtags = null;
		String twitterPropertiesFile = null;
		String topic = null;

		try {
			cmd = setupCommandLine(args);
			brokers = cmd.getOptionValue("brokers");
			String[] hashtagOption = cmd.getOptionValues("hashtags");
			String[] hashtagFileOption = cmd.getOptionValues("hashtag-file");
			twitterPropertiesFile = cmd.getOptionValue("twitter-conf");
			hashtags = processHashTags(hashtagOption, hashtagFileOption);
			topic = cmd.getOptionValue("topic");
		} catch (ParseException e) {
			System.exit(1);
		}

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				TweetSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "150");

		// Kafka producer
		final Producer<String, Tweet> producer = new KafkaProducer<String, Tweet>(
				props);

		if (twitterPropertiesFile == null)
			twitterPropertiesFile = TWITTER4J_PROPERTIES;
		final TwitterStream twitterStream = new TwitterStreamFactory(
				twitterPropertiesFile).getInstance();

		if (topic == null)
			topic = DEFAULT_TOPIC;

		// Listener that is invokes for each tweet matching the filter query
		StatusListener listener = new TweetToProducerListener(topic, producer);
		twitterStream.addListener(listener);

		// create a query to filter the tweets by hashtag and geolocation
		FilterQuery tweetFilterQuery = new FilterQuery();

		// if hashtag list found send it over
		if (hashtags != null && hashtags.length > 0) {
			tweetFilterQuery.track(hashtags);
			twitterStream.filter(tweetFilterQuery);
		}
		// otherwise just read TwitterStream
		else
			twitterStream.sample();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Shutting down producer...");
				producer.close();
				twitterStream.shutdown();
			}
		});

	}

	private static List<String> readFile(String fileName) throws IOException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			List<String> hashtags = new ArrayList<String>();
			String line;

			while ((line = br.readLine()) != null) {
				// split on comma(',')
				String[] splitLine = line.split(",");

				// The first column in the CSV is the hashtag
				String hashtag = splitLine[0];

				hashtags.add(hashtag);
			}
			return hashtags;
		} catch (FileNotFoundException e) {
			log.error(String.format("File not found: %s", fileName), e);
			throw (e);
		} catch (IOException e) {
			log.error(String.format("IO Error: %s", fileName), e);
			throw (e);
		} finally {
			br.close();
		}
	}

	private static String[] processHashTags(String[] hashtagOption,
			String[] hashtagFileOption) {
		List<String> hashtags = new ArrayList<String>();

		if (hashtagOption != null && hashtagOption.length > 0) {
			hashtags.addAll(Arrays.asList(hashtagOption));
		}
		if (hashtagFileOption != null && hashtagFileOption.length > 0) {
			Iterator<String> iterator = Arrays.asList(hashtagFileOption)
					.iterator();
			while (iterator.hasNext()) {
				try {
					hashtags.addAll(readFile(iterator.next()));
				} catch (IOException e) {
				}
			}
		}

		return hashtags.toArray(new String[0]);
	}

	private static CommandLine setupCommandLine(String[] args)
			throws ParseException {
		Options options = new Options();

		options.addOption(Option
				.builder()
				.argName("b")
				.hasArgs()
				.required()
				.longOpt("brokers")
				.desc("Comma-separated list of Kafka brokers in host:port pairs")
				.build());

		options.addOption(Option.builder().argName("h").hasArgs()
				.longOpt("hashtags")
				.desc("Comma-separated list of hashtags to follow").build());

		options.addOption(Option
				.builder()
				.argName("f")
				.hasArgs()
				.longOpt("hashtag-file")
				.desc("File containing list of tags to follow, one per line, optionally with a comma after them")
				.build());

		options.addOption(Option
				.builder()
				.argName("c")
				.hasArg()
				.longOpt("twitter-conf")
				.desc("Path to a twitter4j style properties file containing API keys (defaults to ./twitter4j.properties")
				.build());

		options.addOption(Option.builder().argName("t").hasArg()
				.longOpt("topic").desc("Name of the topic to push tweets to")
				.build());

		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			return cmd;
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("App", options);
			throw (e);
		}

	}
}
