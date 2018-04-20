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
import java.io.Serializable;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.*;

public class TFIDFJobSkills {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("TFIDFJobSkills")
        .getOrCreate();

    // Try reading in all the json files
	String filePath = "hdfs://little-rock:46601/cs455/TP/data/*.json";
	//String filePath = "hdfs://little-rock:46601/cs455/TP/data/*.json";
	String[] jsonFiles = filePath.split(",");
	SparkSession session = SparkSession.builder().getOrCreate();
	Dataset<Row> jobsDataSet = session.read().json(jsonFiles);
	Dataset<Row> reducedDataSet =
        jobsDataSet.drop("positionPeriod").drop("hiringOrganization").drop("normalizedTitle").drop("baseSalary").drop("jobLocation").drop("dateExpires").drop("employmentType").drop("id").drop("incentiveCompensation").drop("jobBenefits").drop("numberOfOpenings").drop("salaryCurrency").drop("specialCommitments").drop("title").drop("url").drop("veteranCommitment").drop("workHours");
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
       for(int i = 0; i < word.size(); i++){
            if(idfValues.length > i)
                values.add(new Tuple2<>(word.get(i), idfValues[i]));
       }
       return values.iterator();
   });

    JavaPairRDD<String, Double> removedDuplicates = features.mapToPair(
        new PairFunction<Tuple2<String, Double>, String, Double>(){
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> tuple){
                return new Tuple2<>(tuple._1().replaceAll("[(,:;.)]", ""), tuple._2());
            }
        }
   );

   PairFunction<Tuple2<String, Tuple2<Double,Double>>, String, Double> getAverageByKey = (tuple) -> {
     Tuple2<Double, Double> val = tuple._2;
     double total = val._1;
     double count = val._2;
     Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
     return averagePair;
  };

   JavaPairRDD<String, Tuple2<Double, Double>> temp1 = removedDuplicates.mapValues(value -> new Tuple2<Double, Double>(value,new Double(1.0)));
   JavaPairRDD<String, Tuple2<Double, Double>> temp2 = temp1.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Double>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
   JavaPairRDD<String, Double> aggregated = temp2.mapToPair(getAverageByKey);

    JavaPairRDD<Double, String> lastRDD =
        aggregated.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>(){
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
                return item.swap();
            }
        });


    ArrayList<String> csList = new CSList().getCSList();

    JavaPairRDD<Double, String> finalRDD = lastRDD.filter( line -> {
        if(csList.contains(line._2.toLowerCase())){
            return true;
        }
        return false;
    });

    finalRDD.sortByKey().saveAsTextFile("hdfs://santa-fe:48800/TP/output/");

    spark.stop();
  }
}

