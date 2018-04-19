package cs455.jobData;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import java.util.Arrays;

public class JobAnalysis{
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("jobAnalysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Graduates File Parsing
        JavaRDD<String> graduatesFile = sc.textFile("hdfs://santa-fe:48800/TP/universityGraduations.csv");

        Function<String, Boolean> csGraduates = line -> {
            try{
                return Integer.parseInt(line.split(",")[1].substring(1,3)) == 11;
            }
            catch(Exception e){
                return false;
            }
        };

        JavaRDD<String> csGrads = graduatesFile.filter(csGraduates);

        Long count = csGrads.count();
        double projectedGradcount = count + (count * .224);
        projectedGradcount = count + (count * .224);
        System.out.println("Projected CS Grads 2018: " + projectedGradcount);

        //Entry level file parsing
        JavaRDD<String> csJobsFile = sc.textFile("hdfs://santa-fe:48800/TP/entryLevelStats.csv");

        Function<String, Integer> csJobs = line -> {
            try{
                String[] values = line.split("\t");
                if(values[0].equals("511200") || values[0].equals("541500")){
                    if(values[2].equals("Doctoral or professional degree") || values[2].equals("Master's Degree")){
                        return 0;
                    }
                    return Integer.parseInt(values[3].replace(",", ""));
                }
                return 0;
            }
            catch(Exception e){
                return 0;
            }
        };

        JavaRDD<Integer> csJobCount = csJobsFile.map(csJobs);
        Integer totalJobCount = csJobCount.reduce((x, y) -> x.intValue()+y.intValue());
        double projectedJobCount = totalJobCount + (totalJobCount * .0778);
        System.out.println("Projected CS Job Count: " + projectedJobCount);
        System.out.println("Projected Job Openings (All Levels): " + (projectedJobCount * .041));
        System.out.println("Projected Entry-Level Job Openings: " + (projectedJobCount * .041 * .2)); 
    }
}