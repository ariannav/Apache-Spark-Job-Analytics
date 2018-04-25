Authors: Arianna Vacca and Colton Larson
Purpose: CS455 Term Project
Date: 24 April 2018
********************************************************************************
                                    README
********************************************************************************

COMPILE
===================
    to compile:
        mvn package

RUN
===================
**NOTE: These programs must be run while Arianna Vacca’s and Colton Larson’s hdfs clusters are running. They must also be run using Arianna’s Spark cluster using her cs.colostate.edu account. 

    Remove Existing HDFS Directory:
        $HADOOP_HOME/bin/hdfs dfs -rm -r /TP/output
    Run Job/Graduate Analysis:
        $SPARK_HOME/bin/spark-submit --class cs455.jobData.JobAnalysis --deploy-mode cluster target/jobData-1.0.jar
    Run Job Description TF-IDF:
        $SPARK_HOME/bin/spark-submit --class cs455.jobData.TFIDFJobSkills --deploy-mode cluster target/jobData-1.0.jar

VIEW OUTPUT
===================
    Navigate to santa-fe.cs.colostate.edu:46626
	Output for Job/Graduate Analysis is under StdOut for the corresponding job. 
    Navigate to santa-fe.cs.colostate.edu:48801
        Output for TF-IDF job is under /TP/output
