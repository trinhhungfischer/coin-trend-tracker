package com.trinhhungfischer.cointrendy.batch;

import com.datastax.spark.connector.util.JavaApiHelper;
import com.trinhhungfischer.cointrendy.common.ProcessorUtils;
import com.trinhhungfischer.cointrendy.common.PropertyFileReader;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Properties;

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
//        var rdd = dataFrame.javaRDD().map();

    }

    private static TweetData transformToTweetData(Row row) {
        TweetData tweetData = new TweetData(
                row.getString(0),
                row.getString(1),
                row.getList(2).stream().map(_.as)
                row.getString(4),
                row.getLong(5),
                row.getLong(6),
                row.getLong(7),
                row.getLong(7),
                row.getTimestamp(8),
                row.getString(9)
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
