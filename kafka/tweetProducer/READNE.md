# Twitter Kafka Producer
Coin Trendy Kafka Producer is Maven application for crawl Tweet data from Twitter using Apache Kafka. This project requires these tools and techonologies:

- JDK - 1.8
- Maven - 3.3.9
- ZooKeeper - 3.4.8
- Kafka - 2.10-0.10.0.0

You can build and run this application using below commands. Please check [resources/twitter-kafka.properties](src/main/java/../resources/twitter-kafka.properties) for configuration details.

```sh
mvn package
mvn exec:java -Dexec.mainClass="TweetDataProducer"
```

Alternate way to run this application is using the twitter-kafka-producer-1.0.jar file created by maven. Open command prompt, go to target folder and execute below command.

```sh
java -jar twitter-kafka-producer-1.0.jar
```
