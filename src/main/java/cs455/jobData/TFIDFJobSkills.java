package cs455.jobData;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.spark.sql.Column;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.ArrayType;
import java.util.Vector;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.collection.JavaConversions;
import org.apache.spark.ml.linalg.SparseVector;

public class TFIDFJobSkills {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("TFIDFJobSkills")
        .getOrCreate();

    // Try reading in all the json files
	String filePath = "hdfs://little-rock:46601/cs455/TP/data/openjobs-jobpostings.apr-2017.json";
	//String filePath = "hdfs://little-rock:46601/cs455/TP/data/*.json";
	String[] jsonFiles = filePath.split(",");
	SparkSession session = SparkSession.builder().getOrCreate();
	Dataset<Row> jobsDataSet = session.read().json(jsonFiles);
	Dataset<Row> reducedDataSet =
        jobsDataSet.drop("positionPeriod").drop("hiringOrganization").drop("normalizedTitle").drop("baseSalary").drop("jobLocation").drop("dateExpires").drop("employmentType").drop("id").drop("incentiveCompensation").drop("jobBenefits").drop("numberOfOpenings").drop("salaryCurrency").drop("specialCommitments").drop("title").drop("url").drop("veteranCommitment").drop("workHours");
    //reducedDataSet.show();
    //reducedDataSet.printSchema();
    Dataset<Row> ocDataSet = reducedDataSet.withColumn("occupationCat", concat_ws(" ", col("occupationalCategory")));
    Dataset<Row> csDataSet = ocDataSet.filter(col("occupationCat").contains("15-11"));
    Dataset<Row> skillsDataSet = csDataSet.select("skills").filter(size(col("skills")).gt(0));
    Dataset<Row> sentences = skillsDataSet.withColumn("sentence", concat_ws(" ", col("skills")));
    Dataset<Row> finalDataSet = sentences.select("sentence");

    JavaRDD<Row> rdd1 = finalDataSet.rdd().toJavaRDD();

    JavaRDD<Row> rdd = rdd1.repartition(1);

    StructType schema = new StructType(new StructField[]{
      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
    });

    Dataset<Row> skills = spark.createDataFrame(rdd, schema);

    Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
    Dataset<Row> wordsData = tokenizer.transform(skills);

    int numFeatures = 64;
    HashingTF hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(numFeatures);

    Dataset<Row> featurizedData = hashingTF.transform(wordsData);

    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
    IDFModel idfModel = idf.fit(featurizedData);

    Dataset<Row> rescaledData = idfModel.transform(featurizedData);

    System.out.println("Transformations:");

    JavaPairRDD<String, Double> features = rescaledData.select("words", "features").rdd().toJavaRDD().flatMapToPair(row -> {
       List<Tuple2<String, Double>> values = new ArrayList<>();
       List<String> word = row.getList(0);
       //Vector data = row.get(1).toArray;
       double[] idfValues = ((SparseVector)row.get(1)).toArray();
       System.out.println(idfValues);
       System.out.flush();
       for(int i = 0; i < word.size(); i++){
           values.add(new Tuple2<>(word.get(i), idfValues[i]));
       }
       return values.iterator();
   });

  features.saveAsTextFile("hdfs://santa-fe:48800/TP/output");

    spark.stop();
  }
}