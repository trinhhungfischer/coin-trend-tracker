package com.trinhhungfischer.cointrendy.batch;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Read from the HDFS the latest processed kafka offset
 * 
 */
public class LatestOffsetReader {
    
    private Dataset<Row> parquetData;

    final SparkSession sparkSession;
    
    final String file;

    public LatestOffsetReader(final SparkSession sparkSession, final String file) {
        this.sparkSession = sparkSession;
        this.file = file;
    }

    public LatestOffsetReader read() {
        parquetData = sparkSession.read().parquet(file);
        return this;
    }

    private JavaRDD<Row> query() throws AnalysisException {
        parquetData.createTempView("tweet");
        return parquetData.sqlContext()
                .sql("select max(untilOffset) as untilOffset, topic, kafkaPartition from tweet group by topic, kafkaPartition")
                .javaRDD();
    }

    public Map<TopicPartition, Long> offsets() throws AnalysisException {
        return this.query()
                .collect()
                .stream()
                .map(LatestOffsetReader::mapToPartition)
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    private static Tuple2<TopicPartition, Long> mapToPartition(Row row) {
        TopicPartition topicPartition = new TopicPartition(
                row.getString(row.fieldIndex("topic")),
                row.getInt(row.fieldIndex("kafkaPartition"))
        );
        Long offSet = Long.valueOf(row.getString(row.fieldIndex("untilOffset")));
        return new Tuple2<>(
                topicPartition,
                offSet
        );
    }
    
    
}
