package com.realtime.spark.risk.tongdun.bak

import com.realtime.spark.utils.JsonMapUtils
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON

/**
 * Describe: 同盾数据解析-解析request的json数据
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object LogAnalyseRequestInfoBak {
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

    //--- request一级json数据中addr_id解析 ---
    val addrIdDetailInfo = analyseAddressIdInfoJson(requestJson)
    requestBuilder.append(addrIdDetailInfo)

    val is_temp_limit = requestJson.get("is_temp_limit")
    requestBuilder.append(is_temp_limit + "\t")

    //--- request中json数据用户地址信息(user_address)解析 ---
    val userAddressDetailInfo = analyseUserAddressInfoJson(requestJson)
    requestBuilder.append(userAddressDetailInfo)

    val random_weight = requestJson.get("random_weight")
    requestBuilder.append(random_weight + "\t")
    val partner_id = requestJson.get("partner_id")
    requestBuilder.append(partner_id + "\t")

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
      val name = requestLevelInfo.get("name")
      requestLevelBuilder.append(name + "\t")
      val user_name = requestLevelInfo.get("user_name")
      requestLevelBuilder.append(user_name + "\t")
      val registed_mobile = requestLevelInfo.get("registed_mobile")
      requestLevelBuilder.append(registed_mobile + "\t")
      val mobile = requestLevelInfo.get("mobile")
      requestLevelBuilder.append(mobile + "\t")
      val id_number = requestLevelInfo.get("id_number")
      requestLevelBuilder.append(id_number + "\t")
      val customer_id = requestLevelInfo.get("customer_id")
      requestLevelBuilder.append(customer_id + "\t")
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
    var user_status = ""
    if (!requestJsonInfo.containsKey("user_status")) {
      user_status = "null"
      userStatusBuilder.append(user_status + "\t")
    } else if (requestJsonInfo.containsKey("user_status")) {
      val user_status_info = requestJsonInfo.get("user_status")
      val userStatusInfoMap = JSON.parseFull(user_status_info.toString)
      userStatusInfoMap match {
        case Some(userInfoMap: Map[String, Any]) => {
          userInfoMap.foreach(userInfoKey => {
            val userInfoValue = userInfoKey._2
            userStatusBuilder.append(userInfoValue + "\t")
          })
        }
        case _ => {
          LOGGER.error("The Request in the json data parsing user_status data error!")
        }
      }
    } else {
      user_status = "null"
      userStatusBuilder.append(user_status + "\t")
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
      val ip_address = requestUserOrderInfo.get("ip_address")
      userOrderBuilder.append(ip_address + "\t")
      val is_test = requestUserOrderInfo.get("is_test")
      userOrderBuilder.append(is_test + "\t")
      val registed_at = requestUserOrderInfo.get("registed_at")
      userOrderBuilder.append(registed_at + "\t")
      val user_type = requestUserOrderInfo.get("user_type")
      userOrderBuilder.append(user_type + "\t")
      val order_type = requestUserOrderInfo.get("order_type")
      userOrderBuilder.append(order_type + "\t")
      val iou_limit = requestUserOrderInfo.get("iou_limit")
      userOrderBuilder.append(iou_limit + "\t")
      val audit_limit = requestUserOrderInfo.get("audit_limit")
      userOrderBuilder.append(audit_limit + "\t")
      val iou_amount = requestUserOrderInfo.get("iou_amount")
      userOrderBuilder.append(iou_amount + "\t")
      val audit_amount = requestUserOrderInfo.get("audit_amount")
      userOrderBuilder.append(audit_amount + "\t")
      val alipay_user_id = requestUserOrderInfo.get("alipay_user_id")
      userOrderBuilder.append(alipay_user_id + "\t")
      val loan_money = requestUserOrderInfo.get("loan_money")
      userOrderBuilder.append(loan_money + "\t")
      val loan_term = requestUserOrderInfo.get("loan_term")
      userOrderBuilder.append(loan_term + "\t")
      val loan_fenqi = requestUserOrderInfo.get("loan_fenqi")
      userOrderBuilder.append(loan_fenqi + "\t")
      val token_id = requestUserOrderInfo.get("token_id")
      userOrderBuilder.append(token_id + "\t")
      val loan_type = requestUserOrderInfo.get("loan_type")
      userOrderBuilder.append(loan_type + "\t")
      val execute_rate = requestUserOrderInfo.get("execute_rate")
      userOrderBuilder.append(execute_rate + "\t")
    }
    catch {
      case _ => {
        LOGGER.error("get request level user order data error!")
      }
    }
    return userOrderBuilder.toString()
  }

  /**
   * 同盾数据解析-request字段用户地址信息(user_address)解析
   * @param requestJson
   * @return
   */
  def analyseUserAddressInfoJson(requestJson: JSONObject): String = {
    val userAddressBuilder = new StringBuilder
    try {
      var user_address = ""
      if (!requestJson.containsKey("user_address")) {
        user_address = "null"
        userAddressBuilder.append(user_address + "\t")
      } else if (requestJson.containsKey("user_address")) {
        val user_address = requestJson.getJSONArray("user_address")
        val userAddressObject = user_address.toJSONObject(user_address)
        val userAddressInfoMap = JsonMapUtils.jsonToMap(userAddressObject.toString())
        userAddressInfoMap.foreach(userAddressKey => {
          val userAddrInfo = JSON.parseFull(userAddressKey._1)
          userAddrInfo match {
            case Some(userAddrValueMap: Map[String, Any]) => {
              userAddrValueMap.keys.foreach(userAddrInfoKey => {
                val userAddrInfoValue = userAddrValueMap(userAddrInfoKey)
                userAddressBuilder.append(userAddrInfoValue + "\t")
              })
            }
            case _ => {
              LOGGER.error("The Request in the json data parsing user_address data error!")
            }
          }
        })
      } else {
        user_address = "null"
        userAddressBuilder.append(user_address + "\t")
      }
    }
    catch {
      case _ => {
        LOGGER.error("The Request user_address analyse error!")
      }
    }

    return userAddressBuilder.toString()
  }


  /**
   * 同盾数据解析-request字段中json一级addr_id信息解析
   * @param requestJson
   * @return
   */
  def analyseAddressIdInfoJson(requestJson: JSONObject): String = {
    val addressIdBuilder = new StringBuilder
    try {
      var addr_id = ""
      if (!requestJson.containsKey("addr_id")) {
        addr_id = "-"
        addressIdBuilder.append(addr_id + "\t")
      } else if (requestJson.containsKey("addr_id")) {
        val addrIdList = requestJson.getJSONArray("addr_id")
        val addrId = addrIdList.toJSONObject(addrIdList)
        val addIdMap = JsonMapUtils.jsonToMap(addrId.toString())
        addIdMap.keys.foreach(addIdKey => {
          var addIdValue = addIdMap(addIdKey)
          if (addIdValue.equals("") || addIdValue == null) {
            addIdValue = "-"
          } else {
            addressIdBuilder.append(addIdValue + "\t")
          }
          println("addIdValue========:" + addIdValue)

        })
      } else {
        addr_id = "-"
        addressIdBuilder.append(addr_id + "\t")

      }
      println("addr_id========:" + addr_id)
    }
    catch {
      case _ => {
        LOGGER.error("The Request address_id analyse error!")
      }
    }
    return addressIdBuilder.toString()
  }
}
