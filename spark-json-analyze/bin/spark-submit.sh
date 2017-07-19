#!/usr/bin/env bash

spark-submit \
 --class com.realtime.spark.risk.das_records.core.LogAnalyseRiskDasRecord \
 /home/hadoop/realtime_program/spark_program/risk_das_records/lib/spark-realtime-1.0-SNAPSHOT.jar

spark-submit \
 --class com.realtime.spark.risk.das_records.core.LogAnalyseRiskDasRecord \
 /home/hadoop/realtime_program/spark_program/risk_das_records/lib/spark-realtime-full.jar


 spark-submit \
 --class com.realtime.spark.risk.das_records.core.LogAnalyseRiskDasRecord \
 /home/hadoop/realtime_program/spark_program/risk_das_records/lib/spark-realtime-full.jar /hive/risk_log/risk_das_records/2017-02-14/s_control_das_score_records/*log


#!/usr/bin/env bash
spark-submit \
 --class com.realtime.spark.risk.tongdun.core.LogAnalyseRiskTongDunInfoMain \
/home/hadoop/test_program/spark_test/spark-tongdun-analyse.jar  /input/tongdun/ /output/tongdun_log





