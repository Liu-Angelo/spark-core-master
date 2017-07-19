package com.realtime.spark.risk.das_records.core

import com.realtime.spark.hadoop.HadoopWriterPlugin
import com.realtime.spark.utils.{JsonMapUtils, DateUtils}
import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/1/19.
 */
object LogAnalyseRiskDasRecord {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val filterRiskDasData = Map("consume_steady_byxs_1y" -> 0, "last_1y_total_active_biz_cnt" -> 0, "flightcount" -> 0, "flightintercount" -> 0,
    "domesticbuscount" -> 0, "domesticfirstcount" -> 0, "avgdomesticdiscount" -> 0, "auth_fin_last_6m_cnt" -> 0, "auth_fin_last_3m_cnt" -> 0,
    "auth_fin_last_1m_cnt" -> 0, "ovd_order_cnt_6m" -> 0, "ovd_order_amt_6m" -> 0, "ovd_order_amt_3m" -> 0, "ovd_order_cnt_1m" -> 0, "sns_pii" -> 0, "relevant_stability" -> 0)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RiskDasRecordLog")
      .setMaster("spark://bi-realname-1:7077")
      .set("spark.executor.memory", "4g")

    val sc = new SparkContext(sparkConf)

    val riskDasLog = sc.textFile(args(0))
    //    val riskDasLog = sc.textFile("/hive/risk_log/risk_das_records/" + DateUtils.getYesterday() + "/s_control_das_score_records/*-" +DateUtils.getYesterday()+ ".log")

    val lines = riskDasLog.map(line =>
      line.split("\t")).map(message =>
      (analyseJsonLog(message(2), message(3), message(4), message(5), message(6), message(7), message(8), message(9), message(10)))
    )
    lines.saveAsTextFile("/hive/stage/s_control_das_score_records")
  }

  def analyseJsonLog(request_id: String, account_id: String, partner_id: String, das_data: String, zm_score: String,
                     score: String, scene: String, create_at: String, update_at: String): String = {
    val builder = new StringBuilder
    var convergeMap = new HashMap[String, Object]()

    builder.append(request_id + "\t")
    builder.append(account_id + "\t")
    builder.append(partner_id + "\t")
    val riskDasJson = JSON.parseFull(das_data)
    riskDasJson match {
      case Some(riskMap: Map[String, Any]) => {
        var varValue = ""
        riskMap("vars") match {
          case varsMap: HashMap[String, String] => {
            varsMap.keys.foreach(key => {
              if (!filterRiskDasData.contains(key)) {
                varValue = varsMap(key)
                if (varValue.equals("")) {
                  varValue = "-"
                }
                convergeMap += (key -> varValue)
              }
            })
            val convergeJson = JsonMapUtils.mapToJson(convergeMap)
            builder.append(convergeJson + "\t")
          }
          case _ => {
            LOGGER.error("The vars in json data is empty...")
          }
        }
      }
      case _ => {
        LOGGER.error("There is no corresponding vars data in das data...")
      }
    }

    var sesame_score = ""
    if (zm_score.equals("[]") || zm_score.isEmpty) {
      sesame_score = "-"
    } else {
      val data = JSONObject.fromObject(zm_score)
      sesame_score = data.getJSONObject("content").getString("score")
    }
    builder.append(sesame_score + "\t")
    builder.append(score + "\t")
    builder.append(scene + "\t")
    builder.append(create_at + "\t")
    builder.append(update_at + "\t")
    val load_time = DateUtils.transHourFormat()
    builder.append(load_time)
    val result = builder.toString()
    result
  }

}
