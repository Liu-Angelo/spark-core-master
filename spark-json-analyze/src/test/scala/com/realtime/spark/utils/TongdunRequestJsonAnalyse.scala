package com.realtime.spark.utils

import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object TongdunRequestJsonAnalyse {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val userStatusData = Map("lfq_user_id" -> 0, "score" -> 0, "app_id" -> 0)

  def main(args: Array[String]) {

    val request_json = "{\"user_id\":\"22428398\",\"name\":\"吴鸿亮\",\"user_name\":\"吴鸿亮\",\"registed_mobile\":\"15872213351\",\"mobile\":\"15872213351\",\"id_number\":\"420602199801182516\",\"customer_id\":\"268813897533674050765156280\",\"user_status\":{\"id\":\"29234496\",\"user_id\":\"20881049666252685831227771012142\",\"alipay_user_id\":\"2088212102663425\",\"cert_no\":\"420602199801182516\",\"real_name\":\"吴鸿亮\",\"avatar\":\"https:\\/\\/tfs.alipayobjects.com\\/images\\/partner\\/T1OSdeXXNkXXXXXXXX\",\"mobile\":\"15872213351\",\"gender\":\"m\",\"user_status\":\"T\",\"user_type_value\":\"2\",\"is_id_auth\":\"T\",\"is_mobile_auth\":\"T\",\"is_bank_auth\":\"T\",\"is_student_certified\":\"F\",\"is_certify_grade_a\":\"T\",\"is_certified\":\"T\",\"is_licence_auth\":\"F\",\"cert_type_value\":\"0\",\"lfq_user_id\":\"22428398\",\"customer_id\":null,\"score\":null,\"alipay_account\":\"2088212102663425\",\"created_at\":\"2017-01-24 19:35:07\",\"updated_at\":\"2017-01-24 19:35:28\",\"deleted_at\":null,\"app_id\":\"1\",\"account_id\":\"0\",\"order_num\":1,\"hasPaid\":1,\"payed_bill_num\":3,\"max_overdue\":0,\"overdue_status\":\"F\",\"redo_bill_num\":0,\"total_overdue_days\":0,\"overdue_times\":0},\"ip_address\":\"183.207.217.99\",\"is_test\":0,\"registed_at\":\"2017-01-24 19:35:26\",\"user_type\":\"normal\",\"order_type\":\"qudian\",\"iou_limit\":\"700.00\",\"audit_limit\":\"2800.00\",\"iou_amount\":\"250.01\",\"audit_amount\":\"250.01\",\"alipay_user_id\":\"2088212102663425\",\"loan_money\":\"400\",\"loan_term\":\"6\",\"loan_fenqi\":\"week\",\"token_id\":\"67a46b4a70f1db72686f0575ac6faee4\",\"loan_type\":1,\"execute_rate\":1,\"addr_id\":[\"65858762\"],\"is_temp_limit\":\"0\",\"user_address\":[{\"id\":\"65858762\",\"prov\":\"江苏省\",\"city\":\"南通市\",\"area\":\"如皋市\",\"mobile\":\"15997181648\",\"name\":\"吴鸿亮\",\"address\":\"长江镇红星北区一二一栋，一零一室\"}],\"random_weight\":698,\"partner_id\":\"10003\"}\t"
    val request_json2 = "{\n    \"user_id\": \"21431791\",\n    \"name\": \"谢艺清\",\n    \"user_name\": \"谢艺清\",\n    \"registed_mobile\": \"13400772122\",\n    \"mobile\": \"13400772122\",\n    \"id_number\": \"35020519891208303X\",\n    \"customer_id\": \"268813897533673839204870554\",\n    \"user_status\": {\n        \"order_num\": 0,\n        \"hasPaid\": 0,\n        \"is_bank_auth\": 0,\n        \"lfq_user_id\": 0\n    },\n    \"ip_address\": \"223.104.6.16\",\n    \"is_test\": 0,\n    \"registed_at\": \"2017-01-13 15:50:36\",\n    \"user_type\": \"normal\",\n    \"order_type\": \"qudian\",\n    \"iou_limit\": 0,\n    \"audit_limit\": 0,\n    \"iou_amount\": 0,\n    \"audit_amount\": 0,\n    \"alipay_user_id\": null,\n    \"token_id\": \"1332294fb50db80585747f2a17b85ba1\",\n    \"random_weight\": 334,\n    \"partner_id\": \"10003\"\n}"
    val json3 = "\n{\n    \"user_id\": \"21431791\",\n    \"name\": \"谢艺清\",\n    \"user_name\": \"谢艺清\",\n    \"registed_mobile\": \"13400772122\",\n    \"mobile\": \"13400772122\",\n    \"id_number\": \"35020519891208303X\",\n    \"customer_id\": \"268813897533673839204870554\",\n    \"ip_address\": \"223.104.6.16\",\n    \"is_test\": 0,\n    \"registed_at\": \"2017-01-13 15:50:36\",\n    \"user_type\": \"normal\",\n    \"order_type\": \"qudian\",\n    \"iou_limit\": 0,\n    \"audit_limit\": 0,\n    \"iou_amount\": 0,\n    \"audit_amount\": 0,\n    \"alipay_user_id\": null,\n    \"token_id\": \"1332294fb50db80585747f2a17b85ba1\",\n    \"random_weight\": 334,\n    \"partner_id\": \"10003\"\n}"
    val json4 = "{\n    \"user_id\": \"21431791\",\n    \"name\": \"谢艺清\",\n    \"user_name\": \"谢艺清\",\n    \"registed_mobile\": \"13400772122\",\n    \"mobile\": \"13400772122\",\n    \"id_number\": \"35020519891208303X\",\n    \"customer_id\": \"268813897533673839204870554\",\n    \"user_status\": {\n        \"order_num\": 0,\n        \"score\":888888,\n        \"lfq_user_id\": 09999,\n        \"hasPaid\": 0\n    },\n    \"ip_address\": \"223.104.6.16\",\n    \"is_test\": 0,\n    \"registed_at\": \"2017-01-13 15:50:36\",\n    \"user_type\": \"normal\",\n    \"order_type\": \"qudian\",\n    \"iou_limit\": 0,\n    \"audit_limit\": 0,\n    \"iou_amount\": 0,\n    \"audit_amount\": 0,\n    \"alipay_user_id\": null,\n    \"token_id\": \"1332294fb50db80585747f2a17b85ba1\",\n    \"random_weight\": 334,\n    \"partner_id\": \"10003\"\n}"
    analyseRequestInfoJson(json4)
  }

  /**
   * 同盾数据解析-request解析主要逻辑
   * @param requestInfo
   */
  def analyseRequestInfoJson(requestInfo: String): String = {
    val requestBuilder = new StringBuilder
    val requestJson = JSONObject.fromObject(requestInfo)

    //--- request一级json数据用户基本信息解析 ---
    val userBasicDetailInfo = requestLevelUserBasicInfo(requestJson)
    requestBuilder.append(userBasicDetailInfo)

    //--- request中json数据用户状态(user_status)解析 ---
    val userStatusDetailInfo = analyseUserStatusInfoJson(requestJson)
    requestBuilder.append(userStatusDetailInfo)

    //--- request一级json数据用户下单信息解析 ---
    val userOrderDetailInfo = analyseUserOrderInfoJson(requestJson)
    requestBuilder.append(userOrderDetailInfo)

    println("*************:" + requestBuilder.toString())

    return requestBuilder.toString()
  }

  /**
   * 同盾数据解析-request字段中json一级用户基本信息解析
   * @param requestLevelInfo
   * @return
   */
  def requestLevelUserBasicInfo(requestLevelInfo: JSONObject): String = {
    val requestLevelBuilder = new StringBuilder
    try {
      val user_id = requestLevelInfo.get("user_id")
      requestLevelBuilder.append(user_id + "\t")
      val user_name = requestLevelInfo.get("user_name")
      requestLevelBuilder.append(user_name + "\t")
      val mobile = requestLevelInfo.get("mobile")
      requestLevelBuilder.append(mobile + "\t")
      val id_number = requestLevelInfo.get("id_number")
      requestLevelBuilder.append(id_number + "\t")
    }
    catch {
      case _ => {
        LOGGER.error("The Request in the json level user basic data parsing error!")
      }
    }

    return requestLevelBuilder.toString()
  }

  /**
   * 同盾数据解析-request字段用户状态信息(user_status)解析
   * @param requestJsonInfo
   * @return
   */
  def analyseUserStatusInfoJson(requestJsonInfo: JSONObject): String = {
    val userStatusBuilder = new StringBuilder
    var userStatusMap = new HashMap[String, Object]
    if (!requestJsonInfo.containsKey("user_status")) {
      userStatusMap +=("gender" -> "-", "lfq_user_id" -> "-", "score" -> "-", "app_id" -> "-")
      val userStatusJson = JsonMapUtils.mapToJson(userStatusMap)
      userStatusBuilder.append(userStatusJson + "\t")
    } else if (requestJsonInfo.containsKey("user_status")) {
      val user_status_info = requestJsonInfo.get("user_status")
      val userStatusInfoMap = JSON.parseFull(user_status_info.toString)
      userStatusInfoMap match {
        case Some(userInfoMap: Map[String, Object]) => {
          userInfoMap.keys.foreach(userInfoKey => {
            if (userInfoKey.contains("gender")) {
              userStatusMap += ("gender" -> userInfoMap.get("gender"))
            } else if (!userInfoKey.contains("gender")) {
              val gender = userInfoMap.getOrElse("gender", "-")
              userStatusMap += ("gender" -> gender)
            }

            if (userInfoKey.contains("lfq_user_id")) {
              userStatusMap += ("lfq_user_id" -> userInfoMap.get("lfq_user_id"))
            } else if (!userInfoKey.contains("lfq_user_id")) {
              val lfq_user_id = userInfoMap.getOrElse("lfq_user_id", "-")
              userStatusMap += ("lfq_user_id" -> lfq_user_id)
            }

            if (userInfoKey.contains("score")) {
              userStatusMap += ("score" -> userInfoMap.get("score"))
            } else if (!userInfoKey.contains("score")) {
              val score = userInfoMap.getOrElse("score", "-")
              userStatusMap += ("score" -> score)
            }

            if (userInfoKey.contains("app_id")) {
              userStatusMap += (userInfoKey -> userInfoMap.get("app_id"))
            } else if (!userInfoKey.contains("app_id")) {
              val app_id = userInfoMap.getOrElse("userInfoMap", "-")
              userStatusMap += ("app_id" -> app_id)
            }

          })
          val userStatusJson = JsonMapUtils.mapToJson(userStatusMap)
          userStatusBuilder.append(userStatusJson + "\t")
        }
        case _ => {
          LOGGER.error("The Request in the json data parsing user_status data error!")
        }
      }
    } else {
      userStatusMap +=("gender" -> "-", "lfq_user_id" -> "-", "score" -> "-", "app_id" -> "-")
      val userStatusJson = JsonMapUtils.mapToJson(userStatusMap)
      userStatusBuilder.append(userStatusJson + "\t")
    }
    return userStatusBuilder.toString()
  }

  /**
   * 同盾数据解析-request字段中json一级用户下单信息解析
   * @param requestUserOrderInfo
   * @return
   */
  def analyseUserOrderInfoJson(requestUserOrderInfo: JSONObject): String = {
    val userOrderBuilder = new StringBuilder
    try {
      val alipay_user_id = requestUserOrderInfo.get("alipay_user_id")
      userOrderBuilder.append(alipay_user_id + "\t")
    }
    catch {
      case _ => {
        LOGGER.error("get request level user order data error!")
      }
    }
    return userOrderBuilder.toString()
  }

}
