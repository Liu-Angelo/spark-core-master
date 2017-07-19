package com.realtime.risk.das_records;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/1/16.
 */
public class LogAnalyseRiskDasRecordMain {

    private static Logger logger = LoggerFactory.getLogger(LogAnalyseRiskDasRecordMain.class);

    public static class AnalyzeJson implements FlatMapFunction<Iterator<String>, RiskDasRecord> {

        public Iterable<RiskDasRecord> call(Iterator<String> lines) throws Exception {
            ArrayList<RiskDasRecord> riskDasRecordsList = new ArrayList<RiskDasRecord>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    riskDasRecordsList.add(mapper.readValue(line, RiskDasRecord.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return riskDasRecordsList;
        }
    }


    public static class LikePandas implements Function<RiskDasRecord, Boolean> {

        public Boolean call(RiskDasRecord riskDasRecord) throws Exception {
            return riskDasRecord.lovesPandas;
        }
    }

    public static class WriteJson implements FlatMapFunction<Iterator<RiskDasRecord>, String> {

        public Iterable<String> call(Iterator<RiskDasRecord> riskDasRecordIterator) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (riskDasRecordIterator.hasNext()) {
                RiskDasRecord riskDasRecord = riskDasRecordIterator.next();
                text.add(mapper.writeValueAsString(riskDasRecord));
            }
            return text;
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
        }

        String master = args[0];
        String inputFile = args[1];
        String outputFile = args[2];

        JavaSparkContext sc = new JavaSparkContext(master, "LogAnalyseRiskDasRecordMain", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<RiskDasRecord> result = input.mapPartitions(new AnalyzeJson()).filter(new LikePandas());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile(outputFile);

    }
}
