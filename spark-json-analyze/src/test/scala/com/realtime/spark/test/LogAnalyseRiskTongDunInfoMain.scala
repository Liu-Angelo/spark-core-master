package com.realtime.spark.test

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/22.
 */
object LogAnalyseRiskTongDunInfoMain extends Serializable {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("AnalyseTunDongLog")
      .setMaster("spark://bi-realname-1:7077")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.cores", "8")
      .set("spark.yarn.driver.memory", "4g")

    val sc = new SparkContext(sparkConf)
    val jobContext = new HiveContext(sc)

    var jobDF = jobContext.read.parquet(args(0))

    jobDF = jobContext.createDataFrame(jobDF.map(x => analyseTongDunJsonLog(x.getString(0))), struct)
    jobDF.show()

    jobDF.save(args(1))
  }

  //  def handleJsonLog(logJson: String): Row = {
  //    val jsonStr = logJson
  //    Row(jsonStr)
  //  }

  def analyseTongDunJsonLog(log: String): Row = {
    val originalLog = log.split("\t")
    val builder = new StringBuilder
    val id = originalLog(0)
    val partner_id = originalLog(1)
    val account_id = originalLog(2)
    val request = originalLog(3)
    val content = originalLog(4)
    val created_at = originalLog(5)
    val updated_at = originalLog(6)
    val token = originalLog(7)
    val zm_customer_id = originalLog(8)
    val requestDetailInfo = LogAnalyseRequestInfo.analyseRequestInfoJson(request)
    val contentDetailInfo = LogAnalyseContentInfo.analyseContentInfoJson(content)
    Row(
      id, partner_id, account_id, requestDetailInfo, contentDetailInfo, created_at, updated_at, token, zm_customer_id
    )
  }

  val struct = StructType(Array(
    StructField("id", StringType),
    StructField("partner_id", StringType),
    StructField("account_id", StringType),
    StructField("requestDetailInfo", StringType),
    StructField("contentDetailInfo", StringType),
    StructField("created_at", StringType),
    StructField("updated_at", StringType),
    StructField("token", StringType),
    StructField("zm_customer_id", StringType)
  ))


  //  val struct_test = StructType(Array(
  //    StructField("jsonStr", StringType)
  //  ))

}
