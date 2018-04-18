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
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration hadoopConfiguration = jsc.hadoopConfiguration();
        JavaRDD<String> miniDataset = jsc.textFile("hdfs://TP/input.txt");
        JavaRDD<String> filterForFirst = miniDataset.filter((String line) -> {
            String summaryLevel = line.substring(10, 13);
            String segmentNo = line.substring(24, 28);
            return summaryLevel.equals("100") && segmentNo.equals("0002");})
            .cache();

        JavaPairRDD<String, Tuple2<Long, Long>> combinedRDD =
        filterForFirst.mapToPair((String line) -> {
        String state = line.substring(8, 10);
        long owned = Long.parseLong(line.substring(1803, 1812));
        long rented = Long.parseLong(line.substring(1812, 1821));
        return new Tuple2<>(state, new Tuple2<>(owned, rented));
        }).reduceByKey((Tuple2<Long, Long> t1, Tuple2<Long, Long>
        t2)
        -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));

        JavaPairRDD<String, Tuple2<Double, Double>> answerRDD =
        combinedRDD.mapValues((Tuple2<Long, Long> t1)
        -> new Tuple2<>(t1._1 * 100d / (t1._1 + t1._2),
        t1._2 * 100d / (t1._1 + t1._2)));
        List<Tuple2<String, Tuple2<Double, Double>>> answer =
        answerRDD.collect();
        for (Tuple2<String, Tuple2<Double, Double>> answer1 : answer) {
        System.out.println(answer1._1 + "\tOWNED:" + answer1._2._1 +
        "\tRENTED:" + answer1._2._2);
        }
    }
}