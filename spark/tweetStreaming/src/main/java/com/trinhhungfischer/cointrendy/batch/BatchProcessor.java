package com.trinhhungfischer.cointrendy.batch;

import com.datastax.spark.connector.util.JavaApiHelper;
import com.trinhhungfischer.cointrendy.common.ProcessorUtils;
import com.trinhhungfischer.cointrendy.common.PropertyFileReader;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Collectors;

public class BatchProcessor {
    public static void main(String[] args) throws Exception {
        String fileProperty = "twitter-spark.properties";
        Properties properties = PropertyFileReader.readPropertyFile(fileProperty);
        properties.setProperty("com.iot.app.spark.app.name", "Tweet Batch Processor");

        var file = properties.getProperty("com.twitter.app.hdfs") + "twitter-data-parquet";
        String[] jars = {properties.getProperty("com.twitter.app.jar")};

        var conf = ProcessorUtils.getSparkConf(properties, "batch-processor");
        conf.setJars(jars);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // Broadcast variables. Basically we are sending the data to each worker nodes on a Spark cluster.
        ClassTag<HashtagData> classTag = JavaApiHelper.getClassTag(HashtagData.class);
        Broadcast<HashtagData> broadcastHashtagsValue = sparkSession
                .sparkContext()
                .broadcast(getHashtagData(), classTag);

        // Batch process starts here
        Dataset<Row> dataFrame = getDataFrame(sparkSession, file);
        JavaRDD<TweetData> rdd = dataFrame.javaRDD().map(BatchProcessor::transformToTweetData);

        BatchTrendingProcessor.processTotalTweetData(rdd, broadcastHashtagsValue);
        BatchTrendingProcessor.processWindowTotalTweetData(rdd, broadcastHashtagsValue);
        sparkSession.close();
        sparkSession.stop();

    }

    private static TweetData transformToTweetData(Row row) {
        TweetData tweetData = new TweetData(
                row.getString(0),
                row.getString(1),
                row.getList (2).stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new)),
                row.getList (3).stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new)),
                row.getString(4),
                row.getLong(5),
                row.getLong(6),
                row.getLong(7),
                row.getLong(8),
                row.getTimestamp(9),
                row.getString(10)
        );
        return tweetData;
    }

    public static Dataset<Row> getDataFrame(SparkSession sqlContext, String file) {
        return sqlContext.read().parquet(file);
    }
    private static HashtagData getHashtagData() {
        HashtagData hashtagData = new HashtagData();
        hashtagData.getHashtags();
        return hashtagData;
    }
}
