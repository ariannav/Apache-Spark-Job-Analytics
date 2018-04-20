//Colton's Code :)

import java.io.StringReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;
import scala.collection.JavaConverters;

// import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import org.apache.spark.sql.Column;

public class JobSkills{
    
    public static void main(String[] args) throws Exception {
    // Try reading in all the json files
		String filePath = "hdfs://little-rock:46601/cs455/TP/data/*.json";
		String[] jsonFiles = filePath.split(",");
		SparkSession session = SparkSession.builder().getOrCreate();
		Dataset<Row> jobsDataSet = session.read().json(jsonFiles);
		Dataset<Row> reducedDataSet = 
            jobsDataSet.drop("positionPeriod").drop("hiringOrganization").drop("normalizedTitle").drop("baseSalary").drop("jobLocation").drop("dateExpires").drop("employmentType").drop("id").drop("incentiveCompensation").drop("jobBenefits").drop("numberOfOpenings").drop("salaryCurrency").drop("specialCommitments").drop("title").drop("url").drop("veteranCommitment").drop("workHours");
        reducedDataSet.show();
        reducedDataSet.printSchema();
        Dataset<Row> skillsDataSet = reducedDataSet.select("skills").filter(size(col("skills")).gt(0));
        skillsDataSet.show();
        
        JavaRDD<Row> rdd = skillsDataSet.rdd().toJavaRDD();
        rdd.saveAsTextFile("hdfs://little-rock:46601/cs455/TP/output/output");
    }
}
