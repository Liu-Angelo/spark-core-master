package com.realtime.risk.das_records;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/1/19.
 */
public class AnalyseRiskDasRecordsLog {
    public static void main(String[] args) {
        //创建SQLContext
        SparkConf conf = new SparkConf()
                .setAppName("AnalyseRiskDasRecordLog")
                .setMaster("spark://bi-realname-1:7077")
                .set("spark.executor.memory", "2g");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        JavaRDD<String> risk_das_log = jsc.textFile("file:///home/hadoop/risk_data/data1/risk_data/2017-01-05/s_control_das_score_records");

        StringBuilder stringBuilder = new StringBuilder();



    }

}