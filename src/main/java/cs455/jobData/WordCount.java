package cs455.jobData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class WordCount{
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("cs455-hw3-spark-example");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        JavaRDD<String> textFile = sc.textFile("hdfs://TP/input.txt");
        JavaPairRDD<String, Integer> counts = textFile
            .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("hdfs://TP/output.txt");
    }
}