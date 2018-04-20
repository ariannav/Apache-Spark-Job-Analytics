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
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.desc;
import org.apache.spark.sql.Column;

public class JobSkills{
    
    public static void main(String[] args) throws Exception {
        // Try reading in all the json files
		String filePath = "hdfs://little-rock:46601/cs455/TP/data/openjobs-jobpostings.apr-2017.json";
		//String filePath = "hdfs://little-rock:46601/cs455/TP/data/*.json";
		String[] jsonFiles = filePath.split(",");
		SparkSession session = SparkSession.builder().getOrCreate();
		Dataset<Row> jobsDataSet = session.read().json(jsonFiles);
		Dataset<Row> reducedDataSet = 
            jobsDataSet.drop("positionPeriod").drop("hiringOrganization").drop("normalizedTitle").drop("baseSalary").drop("jobLocation").drop("dateExpires").drop("employmentType").drop("id").drop("incentiveCompensation").drop("jobBenefits").drop("numberOfOpenings").drop("salaryCurrency").drop("specialCommitments").drop("title").drop("url").drop("veteranCommitment").drop("workHours");
        reducedDataSet.show();
        reducedDataSet.printSchema();
        Dataset<Row> ocDataSet = reducedDataSet.withColumn("occupationCat", concat_ws(" ", col("occupationalCategory")));
        Dataset<Row> csDataSet = ocDataSet.filter(col("occupationCat").contains("15-11"));
        Dataset<Row> skillsDataSet = csDataSet.select("skills").filter(size(col("skills")).gt(0));
        Dataset<Row> sentences = skillsDataSet.withColumn("sentence", concat_ws(" ", col("skills")));
        Dataset<Row> finalDataSet = sentences.select("sentence");
        
        JavaRDD<Row> rdd = finalDataSet.rdd().toJavaRDD();
//         for(Row row: rdd.collect()){
//             System.out.print(row.toString());
//         }
        rdd.repartition(1).saveAsTextFile("hdfs://little-rock:46601/cs455/TP/output/output");
        //-----------------------------------------------------------------------------------------------
    }
}
