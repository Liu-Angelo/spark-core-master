package com.realtime.spark.test

import com.realtime.spark.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/1/19.
 */
object LogAnalyseRiskDasRecord extends App {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val filterRiskDasData = Map("consume_steady_byxs_1y" -> 0, "last_1y_total_active_biz_cnt" -> 0, "flightcount" -> 0, "flightintercount" -> 0,
    "domesticbuscount" -> 0, "domesticfirstcount" -> 0, "avgdomesticdiscount" -> 0, "auth_fin_last_6m_cnt" -> 0, "auth_fin_last_3m_cnt" -> 0,
    "auth_fin_last_1m_cnt" -> 0, "ovd_order_cnt_6m" -> 0, "ovd_order_amt_6m" -> 0, "ovd_order_amt_3m" -> 0, "ovd_order_cnt_1m" -> 0)

  val conf = new SparkConf()
    .setAppName("AnalyseRiskDasRecordLog")
    .setMaster("spark://bi-realname-1:7077")
    .set("spark.executor.memory", "2g")

  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val risk_das_log = sc.textFile("/input/risk_das_records/10.161.144.20-2017-01-05.log")

  val lines = risk_das_log.map(line =>
    line.split("\t")).map(message =>
    (analyseJsonLog(message(2), message(3), message(4), message(5), message(6), message(7), message(8), message(9), message(10)))
  )

  lines.saveAsTextFile("/output/risk_das_records")


  def analyseJsonLog(request_id: String, account_id: String, partner_id: String, das_data: String, zm_score: String,
                     score: String, scene: String, create_at: String, update_at: String): String = {
    val builder = new StringBuilder
    builder.append(request_id + "\t")
    builder.append(account_id + "\t")
    builder.append(partner_id + "\t")

    val riskDasJson = JSON.parseFull(das_data)
    riskDasJson match {
      case Some(riskMap: Map[String, Any]) => {
        riskMap("vars") match {
          case varsMap: HashMap[String, String] => {
            varsMap.keys.foreach(key => {
              if (!filterRiskDasData.contains(key)) {
                var varValue = varsMap(key)
                if (varValue.equals("")) {
                  varValue = "-"
                }
                builder.append(varValue + "\t")
              }

            })
          }
          case _ => {
            println("没有vars对应的json数据为空")
            LOGGER.error("没有vars对应的json数据为空")
          }
        }
      }

      case _ => {
        println("das数据中没有对应的vars数据")
        LOGGER.error("das数据中没有对应的vars数据")
      }
    }

    builder.append(zm_score + "\t")
    builder.append(score + "\t")
    builder.append(scene + "\t")
    builder.append(create_at + "\t")
    builder.append(update_at + "\t")

    val result = builder.toString()

    result
  }
  
}
