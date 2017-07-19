package com.realtime.spark.risk.tongdun.core

import com.realtime.spark.utils.JsonMapUtils
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: 同盾数据解析-解析request的json数据
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object LogAnalyseRequestInfo {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  /**
   * 同盾数据解析-content解析主要逻辑
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