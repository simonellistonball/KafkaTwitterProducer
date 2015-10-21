Twitter Stream -> Kafka Producer
================================

This is a quick tool to follow hash tags on twitter and push them into Kafka. 


Build instructions
------------------

    mvn clean package


How to use
----------

Create a twitter4j.properties file containing: 

    oauth.consumerKey=
    oauth.consumerSecret=
    oauth.accessToken=
    oauth.accessTokenSecret=

These values come from the [twitter API page](https://apps.twitter.com/). 

The topic defaults to "tweets", so you will need to create that, e.g.:

    /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic tweets --partitions 3 --replication-factor 3 --zookeeper <zks>


Run with the following:

    > java -jar KafkaTwitterProducer-0.0.1-SNAPSHOT.jar
    usage: App
         --brokers <b>        Comma-separated list of Kafka brokers in
                              host:port pairs
         --hashtag-file <f>   Comma-separated list of Kafka brokers in
                              host:port pairs
         --hashtags <h>       Comma-separated list of hashtags to follow
         --topic <t>          Name of the topic to push tweets to
         --twitter-conf <t>   Path to a twitter4j style properties file
                              containing API keys (defaults to
                              ./twitter4j.properties
                             
