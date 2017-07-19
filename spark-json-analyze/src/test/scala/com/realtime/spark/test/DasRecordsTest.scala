package com.realtime.spark.test


import com.realtime.spark.utils.{JsonMapUtils}

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON


/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/8.
 */
object DasRecordsTest {
  val filterRiskDasData = Map("consume_steady_byxs_1y" -> 0, "last_1y_total_active_biz_cnt" -> 0, "flightcount" -> 0, "flightintercount" -> 0,
    "domesticbuscount" -> 0, "domesticfirstcount" -> 0, "avgdomesticdiscount" -> 0, "auth_fin_last_6m_cnt" -> 0, "auth_fin_last_3m_cnt" -> 0,
    "auth_fin_last_1m_cnt" -> 0, "ovd_order_cnt_6m" -> 0, "ovd_order_amt_6m" -> 0, "ovd_order_amt_3m" -> 0, "ovd_order_cnt_1m" -> 0, "sns_pii" -> 0, "relevant_stability" -> 0)

  def main(args: Array[String]): Unit = {
    val builder = new StringBuilder
    val das_data = "{\n    \"vars\": {\n        \"relevant_stability\": \"07\",\n        \"sns_pii\": \"05\",\n        \"occupation\": \"无法识别\",\n        \"company_name\": \"山西物化院(模型预测)\",\n        \"consume_steady_byxs_1y\": \"08\",\n        \"use_mobile_2_cnt_1y\": \"01\",\n        \"mobile_fixed_days\": \"07\",\n        \"adr_stability_days\": \"04\",\n        \"activity_area_stability\": \"02\",\n        \"last_1y_total_active_biz_cnt\": \"07\",\n        \"flightcount\": \"#\",\n        \"flightintercount\": \"#\",\n        \"domesticbuscount\": \"#\",\n        \"domesticfirstcount\": \"#\",\n        \"avgdomesticdiscount\": \"#\",\n        \"have_car_flag\": \"01\",\n        \"have_fang_flag\": \"01\",\n        \"last_1y_avg_asset_total\": \"04\",\n        \"last_3m_avg_asset_total\": \"05\",\n        \"last_1m_avg_asset_total\": \"05\",\n        \"last_6m_avg_asset_total\": \"04\",\n        \"tot_pay_amt_3m\": \"03\",\n        \"tot_pay_amt_1m\": \"01\",\n        \"ebill_pay_amt_6m\": \"03\",\n        \"ebill_pay_amt_3m\": \"04\",\n        \"ebill_pay_amt_1m\": \"01\",\n        \"avg_puc_sdm_last_1y\": \"01\",\n        \"pre_1y_pay_cnt\": \"07\",\n        \"pre_1y_pay_amount\": \"04\",\n        \"tot_pay_amt_6m\": \"03\",\n        \"xfdc_index\": \"04\",\n        \"auth_fin_last_6m_cnt\": \"03\",\n        \"auth_fin_last_3m_cnt\": \"03\",\n        \"auth_fin_last_1m_cnt\": \"02\",\n        \"credit_pay_amt_1y\": \"02\",\n        \"credit_pay_amt_3m\": \"02\",\n        \"credit_pay_amt_1m\": \"03\",\n        \"credit_pay_months_1y\": \"07\",\n        \"credit_total_pay_months\": \"05\",\n        \"credit_duration\": \"03\",\n        \"credit_pay_amt_6m\": \"02\",\n        \"positive_biz_cnt_1y\": \"02\",\n        \"ovd_order_cnt_2y_m3_status\": \"N\",\n        \"ovd_order_cnt_2y_m6_status\": \"N\",\n        \"ovd_order_cnt_6m\": \"01\",\n        \"ovd_order_amt_6m\": \"01\",\n        \"ovd_order_cnt_6m_m1_status\": \"N\",\n        \"ovd_order_cnt_3m\": \"01\",\n        \"ovd_order_amt_3m\": \"01\",\n        \"ovd_order_cnt_3m_m1_status\": \"N\",\n        \"ovd_order_cnt_5y_m3_status\": \"N\",\n        \"ovd_order_cnt_5y_m6_status\": \"N\",\n        \"ovd_order_cnt_1m\": \"01\",\n        \"ovd_order_amt_1m\": \"01\",\n        \"ovd_order_cnt_12m\": \"01\",\n        \"ovd_order_amt_12m\": \"01\",\n        \"ovd_order_cnt_12m_m1_status\": \"N\",\n        \"ovd_order_cnt_12m_m3_status\": \"N\",\n        \"ovd_order_cnt_12m_m6_status\": \"N\"\n    },\n    \"credit_data_id\": 1229689758\n}"
    val riskDasJson = JSON.parseFull(das_data)
    var convergeMap = new HashMap[String, Object]()

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
                builder.append(varValue + "\t")
              }

            })
            val convergeJson = JsonMapUtils.mapToJson(convergeMap)

            println("das拆分json数据******" + builder)
            println("聚合map********：" + convergeMap)
            println("聚合json数据********：" + convergeJson)
          }

          case _ => {
            println("没有vars对应的json数据为空")
          }
        }
      }

      case _ => {
        println("das数据中没有对应的vars数据")
      }
    }

    println()
  }


}
