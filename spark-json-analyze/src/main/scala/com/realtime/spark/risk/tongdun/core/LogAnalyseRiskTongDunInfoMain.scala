package com.realtime.spark.risk.tongdun.core

import com.realtime.spark.utils.{DateUtils, JsonMapUtils}
import net.sf.json.JSONObject
import org.apache.hadoop.hive.ql.io.RCFileInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable

import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

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
      .set("spark.driver.cores", "4")
      .set("spark.yarn.driver.memory", "4g")

    val sc = new SparkContext(sparkConf)

    val riskTongDunLog = sc.textFile(args(0))

    val lines = riskTongDunLog.map(line =>
      line.split("\t")).map(message =>
      (analyseTongDunCombinerInfo(message(0).toInt, message(1), message(2), message(3), message(4), message(5),
        message(6), message(7), message(8)))
    )
    lines.filter(f => !f.equals("") && f != null).saveAsTextFile(args(1))
  }

  def analyseTongDunCombinerInfo(id: Int, partner_id: String, account_id: String, request: String,
                                 content: String, created_at: String, updated_at: String, token: String, zm_customer_id: String): String = {
    val combinerInfoBuilder = new StringBuilder
    val extraFieldInfo = analyseTongDunJsonLog(id, partner_id, account_id)

    val timeFieldInfo = combineTimeInfo(created_at, updated_at, token, zm_customer_id)

    val requestAndContent = analyseTongDunRequestAndContentLog(extraFieldInfo, request, content, timeFieldInfo)
    combinerInfoBuilder.append(requestAndContent)

    return combinerInfoBuilder.toString()
  }

  def analyseTongDunJsonLog(id: Int, partner_id: String, account_id: String): String = {
    val builder = new StringBuilder
    builder.append(id + "\t")
    builder.append(partner_id + "\t")
    builder.append(account_id + "\t")

    val result = builder.toString()
    return result
  }

  def analyseTongDunRequestAndContentLog(extraFieldInfo: String, request: String, content: String, timeInfo: String): String = {
    val requestAndContentBuilder = new StringBuilder

    val contentDetailInfo = LogAnalyseContentInfo.analyseContentInfoJson(extraFieldInfo, request, content, timeInfo)
    requestAndContentBuilder.append(contentDetailInfo)

    return requestAndContentBuilder.toString()
  }

  def combineTimeInfo(created_at: String, updated_at: String, token: String, zm_customer_id: String): String = {
    val timeInfoBuilder = new StringBuilder
    timeInfoBuilder.append(created_at + "\t")
    timeInfoBuilder.append(updated_at + "\t")
    timeInfoBuilder.append(token + "\t")
    timeInfoBuilder.append(zm_customer_id + "\t")
    val load_time = DateUtils.transHourFormat()
    timeInfoBuilder.append(load_time)

    return timeInfoBuilder.toString()
  }


}
