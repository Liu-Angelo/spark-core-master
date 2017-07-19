package com.realtime.spark.utils

import java.util.ArrayList

import net.sf.json.JSONObject
import org.slf4j.LoggerFactory
import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/23.
 */
object TongdunContentJsonAnalyse {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val policySetMessage = Map("policy_decision" -> 0, "policy_mode" -> 0, "policy_name" -> 0, "policy_score" -> 0, "policy_uuid" -> 0, "risk_type" -> 0)
  val hitRulesMessage = Map("hit_rules" -> 0)
  val filterHitRulesField = Map("uuid" -> 0, "score" -> 0, "decision" -> 0, "parentUuid" -> 0)

  def main(args: Array[String]) {
    val request_json = "{\n    \"user_id\": \"21431791\",\n    \"name\": \"谢艺清\",\n    \"user_name\": \"谢艺清\",\n    \"registed_mobile\": \"13400772122\",\n    \"mobile\": \"13400772122\",\n    \"id_number\": \"35020519891208303X\",\n    \"customer_id\": \"268813897533673839204870554\",\n    \"user_status\": {\n        \"order_num\": 0,\n        \"score\":888888,\n        \"lfq_user_id\": 09999,\n        \"hasPaid\": 0\n    },\n    \"ip_address\": \"223.104.6.16\",\n    \"is_test\": 0,\n    \"registed_at\": \"2017-01-13 15:50:36\",\n    \"user_type\": \"normal\",\n    \"order_type\": \"qudian\",\n    \"iou_limit\": 0,\n    \"audit_limit\": 0,\n    \"iou_amount\": 0,\n    \"audit_amount\": 0,\n    \"alipay_user_id\": null,\n    \"token_id\": \"1332294fb50db80585747f2a17b85ba1\",\n    \"random_weight\": 334,\n    \"partner_id\": \"10003\"\n}"

    val content_json = "{\n    \"device_info\": {\n        \"code\": \"061\",\n        \"success\": false,\n        \"message\": \"查无结果\"\n    },\n    \"final_decision\": \"Accept\",\n    \"final_score\": 6,\n    \"hit_rules\": [\n        {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564342\",\n                    \"name\": \"标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                },\n                {\n                    \"decision\": \"success\",\n                    \"id\": \"333333\",\n                    \"name\": \"借款时设备\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                },\n                {\n                    \"decision\": \"fail\",\n                    \"id\": \"22222\",\n                    \"name\": \"借款时设备标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 7,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                }\n    ],\n    \"policy_name\": \"借款策略集\",\n    \"policy_set\": [\n        {\n            \"hit_rules\": [\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564342\",\n                    \"name\": \"标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                },\n                {\n                    \"decision\": \"success\",\n                    \"id\": \"333333\",\n                    \"name\": \"借款时设备\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                },\n                {\n                    \"decision\": \"fail\",\n                    \"id\": \"22222\",\n                    \"name\": \"借款时设备标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 7,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                }\n            ],\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"异常借款\",\n            \"policy_score\": 6,\n            \"policy_uuid\": \"d77e0c204c0849b9acbe47697a964b4d\",\n            \"risk_type\": \"suspiciousLoan\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"机构代办\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"bf47f5fe4c3d4f69bbed15771e5b1497\",\n            \"risk_type\": \"agency\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"WorstMatch\",\n            \"policy_name\": \"失信借款\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"261b115400d94f4b9af8735c39137a0e\",\n            \"risk_type\": \"creditRisk\"\n        }\n    ],\n    \"policy_set_name\": \"借款策略集\",\n    \"risk_type\": \"\",\n    \"seq_id\": \"1488124801206213S180F67FE8356843\",\n    \"spend_time\": 64,\n    \"success\": true,\n    \"event_id\": \"loan_web\"\n}"
    val content_json2 = "{\n    \"device_info\": {\n        \"code\": \"061\",\n        \"success\": false,\n        \"message\": \"查无结果\"\n    },\n    \"final_decision\": \"Accept\",\n    \"final_score\": 6,\n    \"hit_rules\": [\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564342\",\n            \"name\": \"借款时设备标示缺失异常\",\n            \"parentUuid\": \"\",\n            \"score\": 6,\n            \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n        }\n    ],\n    \"policy_name\": \"借款策略集\",\n    \"policy_set\": [\n        {\n            \"hit_rules\": [\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564342\",\n                    \"name\": \"借款时设备标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                }\n            ],\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"异常借款\",\n            \"policy_score\": 6,\n            \"policy_uuid\": \"d77e0c204c0849b9acbe47697a964b4d\",\n            \"risk_type\": \"suspiciousLoan\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"机构代办\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"bf47f5fe4c3d4f69bbed15771e5b1497\",\n            \"risk_type\": \"agency\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"WorstMatch\",\n            \"policy_name\": \"失信借款\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"261b115400d94f4b9af8735c39137a0e\",\n            \"risk_type\": \"creditRisk\"\n        }\n    ],\n    \"policy_set_name\": \"借款策略集\",\n    \"risk_type\": \"\",\n    \"seq_id\": \"1488124801206213S180F67FE8356843\",\n    \"spend_time\": 64,\n    \"success\": true,\n    \"event_id\": \"loan_web\"\n}"
    val json3 = "{\n    \"device_info\": {\n        \"code\": \"061\",\n        \"success\": false,\n        \"message\": \"查无结果\"\n    },\n    \"final_decision\": \"Accept\",\n    \"final_score\": 6,\n    \"hit_rules\": [\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564342\",\n            \"name\": \"借款时设备标示缺失异常\",\n            \"parentUuid\": \"\",\n            \"score\": 6,\n            \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n        }\n    ],\n    \"policy_name\": \"借款策略集\",\n    \"policy_set\": [\n        {\n            \"hit_rules\": [\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564342\",\n                    \"name\": \"借款时设备标示缺失异常\",\n                    \"parentUuid\": \"\",\n                    \"score\": 6,\n                    \"uuid\": \"5e5cbbd23f214822b54b25824add9162\"\n                }\n            ],\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"异常借款\",\n            \"policy_score\": 6,\n            \"policy_uuid\": \"d77e0c204c0849b9acbe47697a964b4d\",\n            \"risk_type\": \"suspiciousLoan\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"机构代办\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"bf47f5fe4c3d4f69bbed15771e5b1497\",\n            \"risk_type\": \"agency\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"WorstMatch\",\n            \"policy_name\": \"失信借款\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"261b115400d94f4b9af8735c39137a0e\",\n            \"risk_type\": \"creditRisk\"\n        }\n    ],\n    \"policy_set_name\": \"借款策略集\",\n    \"risk_type\": \"\",\n    \"seq_id\": \"1488124801206213S180F67FE8356843\",\n    \"spend_time\": 64,\n    \"success\": true,\n    \"event_id\": \"loan_web\"\n}"
    analyseContentInfoJson(content_json, request_json)

  }

  /**
   * 同盾数据-content详情解析
   * @param contentInfo
   */
  def analyseContentInfoJson(contentInfo: String, requestInfo: String): String = {
    val contentBuilder = new StringBuilder
    val contentJson = JSONObject.fromObject(contentInfo)

    //--- content中json数据设备信息(device_info)解析 ---
    val deviceDetailInfo = analyseDeviceInfoJson(contentJson)
    contentBuilder.append(deviceDetailInfo)

    val final_decision = contentJson.get("final_decision")
    contentBuilder.append(final_decision + "\t")
    val final_score = contentJson.get("final_score")
    contentBuilder.append(final_score + "\t")

    //--- 策略集信息解析 ---
    val policy_name = contentJson.get("policy_name")
    contentBuilder.append(policy_name + "\t")

    /*    val policySetDetailInfo = analysePolicySetInfoJson(contentJson)
        contentBuilder.append(policySetDetailInfo)*/

    //--- 其他一级json数据解析 ---
    val otherLevelDetailInfo = analyseContentOtherInfoJson(contentJson)
    contentBuilder.append(otherLevelDetailInfo)

    val requestAnalyseDetailInfo = TongdunRequestJsonAnalyse.analyseRequestInfoJson(requestInfo)

    //--- 命中规则信息解析 ---
    val hitRulesDetailInfo = analyseHitRulesInfoJson(requestAnalyseDetailInfo, contentBuilder.toString(), contentJson)

    val requestAndContentInfo = hitRulesDetailInfo.toString
    println("^^^^^^^^^^^^^^:" + requestAndContentInfo)
    return requestAndContentInfo

  }

  /**
   * 同盾数据解析-content字段中设备信息(deviceInfo)解析
   * @param deviceInfo
   * @return
   */
  def analyseDeviceInfoJson(deviceInfo: JSONObject): String = {
    val deviceInfoBuilder = new StringBuilder
    val device = deviceInfo.get("device_info")
    val device_info = JSON.parseFull(device.toString)
    var geoIPInfoMap = new HashMap[String, Object]
    val deviceInfoLevel = deviceInfo.getJSONObject("device_info")

    val deviceType = deviceInfoLevel.get("deviceType")
    deviceInfoBuilder.append(deviceType + "\t")
    val referer = deviceInfoLevel.get("referer")
    deviceInfoBuilder.append(referer + "\t")

    val geoIP = deviceInfoLevel.get("geoIp")
    if (geoIP == null || geoIP.equals("")) {
      geoIPInfoMap +=("address" -> "-", "area" -> "-", "areaId" -> "-", "city" -> "-", "cityId" -> "-", "country" -> "-",
        "countryId" -> "-", "county" -> "-", "countyId" -> "-", "province" -> "-", "provinceId" -> "-", "type" -> "-")
      val deviceInfoObjectJson = JsonMapUtils.mapToJson(geoIPInfoMap)
      deviceInfoBuilder.append(deviceInfoObjectJson + "\t")
    } else {
      val geoIPInfoObjectMap = JSON.parseFull(geoIP.toString)
      geoIPInfoObjectMap match {
        case Some(geoIpValueMap: Map[String, Object]) => {
          geoIpValueMap.keys.foreach(geoIPKey => {
            if (geoIPKey.contains("address")) {
              geoIPInfoMap += ("address" -> geoIpValueMap.get("address"))
            } else if (!geoIPKey.contains("address")) {
              val address = geoIpValueMap.getOrElse("address", "-")
              geoIPInfoMap += ("address" -> address)
            }

            if (geoIPKey.contains("area")) {
              geoIPInfoMap += ("area" -> geoIpValueMap.get("area"))
            } else if (!geoIPKey.contains("area")) {
              val area = geoIpValueMap.getOrElse("area", "-")
              geoIPInfoMap += ("area" -> area)
            }

            if (geoIPKey.contains("areaId")) {
              geoIPInfoMap += ("areaId" -> geoIpValueMap.get("areaId"))
            } else if (!geoIPKey.contains("areaId")) {
              val areaId = geoIpValueMap.getOrElse("areaId", "-")
              geoIPInfoMap += ("areaId" -> areaId)
            }

            if (geoIPKey.contains("city")) {
              geoIPInfoMap += ("city" -> geoIpValueMap.get("geoIpValueMap"))
            } else if (!geoIPKey.contains("city")) {
              val city = geoIpValueMap.getOrElse("city", "-")
              geoIPInfoMap += ("city" -> city)
            }

            if (geoIPKey.contains("cityId")) {
              geoIPInfoMap += ("cityId" -> geoIpValueMap.get("cityId"))
            } else if (!geoIPKey.contains("cityId")) {
              val cityId = geoIpValueMap.getOrElse("cityId", "-")
              geoIPInfoMap += ("cityId" -> cityId)
            }

            if (geoIPKey.contains("country")) {
              geoIPInfoMap += ("cityId" -> geoIpValueMap.get("country"))
            } else if (!geoIPKey.contains("country")) {
              val country = geoIpValueMap.getOrElse("country", "-")
              geoIPInfoMap += ("country" -> country)
            }

            if (geoIPKey.contains("countryId")) {
              geoIPInfoMap += ("countryId" -> geoIpValueMap.get("countryId"))
            } else if (!geoIPKey.contains("countryId")) {
              val countryId = geoIpValueMap.getOrElse("countryId", "-")
              geoIPInfoMap += ("countryId" -> countryId)
            }

            if (geoIPKey.contains("county")) {
              geoIPInfoMap += ("county" -> geoIpValueMap.get("county"))
            } else if (!geoIPKey.contains("county")) {
              val county = geoIpValueMap.getOrElse("county", "-")
              geoIPInfoMap += ("county" -> county)
            }

            if (geoIPKey.contains("countyId")) {
              geoIPInfoMap += ("countyId" -> geoIpValueMap.get("countyId"))
            } else if (!geoIPKey.contains("countyId")) {
              val countyId = geoIpValueMap.getOrElse("countyId", "-")
              geoIPInfoMap += ("countyId" -> countyId)
            }

            if (geoIPKey.contains("province")) {
              geoIPInfoMap += ("province" -> geoIpValueMap.get("province"))
            } else if (!geoIPKey.contains("province")) {
              val province = geoIpValueMap.getOrElse("province", "-")
              geoIPInfoMap += ("province" -> province)
            }

            if (geoIPKey.contains("provinceId")) {
              geoIPInfoMap += ("provinceId" -> geoIpValueMap.get("provinceId"))
            } else if (!geoIPKey.contains("provinceId")) {
              val provinceId = geoIpValueMap.getOrElse("provinceId", "-")
              geoIPInfoMap += ("provinceId" -> provinceId)
            }

            if (geoIPKey.contains("type")) {
              geoIPInfoMap += ("type" -> geoIpValueMap.get("type"))
            } else if (!geoIPKey.contains("type")) {
              val risk_type = geoIpValueMap.getOrElse("type", "-")
              geoIPInfoMap += ("type" -> risk_type)
            }
          })
          val deviceInfoObjectJson = JsonMapUtils.mapToJson(geoIPInfoMap)
          deviceInfoBuilder.append(deviceInfoObjectJson + "\t")
        }
        case _ => {
          LOGGER.error("The Request in the json data parsing geoIp data error!")
        }
      }
    }

    val tokenId = deviceInfoLevel.get("tokenId")
    deviceInfoBuilder.append(tokenId + "\t")
    val os = deviceInfoLevel.get("os")
    deviceInfoBuilder.append(os + "\t")
    val userAgent = deviceInfoLevel.get("userAgent")
    deviceInfoBuilder.append(userAgent + "\t")
    val version = deviceInfoLevel.get("version")
    deviceInfoBuilder.append(version + "\t")
    val deviceId = deviceInfoLevel.get("deviceId")
    deviceInfoBuilder.append(deviceId + "\t")

    return deviceInfoBuilder.toString()
  }


  /**
   * 同盾数据解析-content字段中命中规则信息(policy_set)解析
   * @param contentJson
   * @return
   */
  def analysePolicySetInfoJson(contentJson: JSONObject): String = {
    val policySetBuilder = new StringBuilder
    var policy_set = ""
    if (!contentJson.containsKey("policy_set")) {
      policy_set = "null"
      policySetBuilder.append(policy_set + "\t")
    } else if (contentJson.containsKey("policy_set")) {
      val policy_setInfo = contentJson.getJSONArray("policy_set")


      val isHitRulesInfoBuilder = new StringBuilder

      val policySetInfo = JSON.parseFull(policy_setInfo.toString)

      policySetInfo match {
        case Some(policySetInfoList: List[Map[String, Any]]) => {
          for (policySetInfoMap <- policySetInfoList) {

            if (policySetInfoMap.contains("hit_rules")) {
              policySetInfoMap.foreach(isHitRulesKey => {
                //                println("^^^^^^^^^^^^^^" + policySetInfoMap)
                if (policySetMessage.contains(isHitRulesKey._1)) {
                  var policySetDetailMessage = isHitRulesKey._2
                  isHitRulesInfoBuilder.append(policySetDetailMessage + "\t")
                }
                if (hitRulesMessage.contains(isHitRulesKey._1)) {
                  val hitRulesMessage = isHitRulesKey._2
                  val hitRulesMessageList = new ArrayList[String]()
                  hitRulesMessageList.add(hitRulesMessage.toString)
                  //                  println("8888888888888:" + hitRulesMessageList)
                }
              })
            }

          }
        }
        case _ => {
          println("解析数据有错误，请检查！")
        }
      }

      val policySetObject = policy_setInfo.toJSONObject(policy_setInfo)
      val policySetInfoMap = JsonMapUtils.jsonToMap(policySetObject.toString())

    } else {
      policy_set = "null"
      policySetBuilder.append(policy_set + "\t")
    }

    return policySetBuilder.toString()
  }


  /**
   * 同盾数据解析-content字段中命中策略集中的规则信息(hit_rules)解析
   * @param contentJson
   * @return
   */
  def analyseHitRulesInfoJson(requestInfo: Object, combine: String, contentJson: JSONObject): String = {
    val hitRulesJsonInfoBuilder = new StringBuilder
    val hitRulesMessageBuilder = new StringBuilder
    var hitRulesJsonMap = new HashMap[String, Object]
    try {
      if (!contentJson.containsKey("hit_rules")) {
        hitRulesJsonMap +=("name" -> "-", "uuid" -> "-", "score" -> "-", "decision" -> "-", "id" -> "-", "parentUuid" -> "-")
        val hitRulesNullInfoJson = JsonMapUtils.mapToJson(hitRulesJsonMap)
        hitRulesJsonInfoBuilder.append(requestInfo + combine + hitRulesNullInfoJson)
      } else if (contentJson.containsKey("hit_rules")) {
        val hitRulesInfo = contentJson.getJSONArray("hit_rules")
        val hitRulesMessage = JSON.parseFull(hitRulesInfo.toString)
        hitRulesMessage match {
          case Some(hitRulesList: List[Map[String, Object]]) => {
            for (hitRulesMap <- hitRulesList) {
              var hitRulesInfoJson = ""
              hitRulesMap.foreach(hitRulesMessage => {
                if (!filterHitRulesField.contains(hitRulesMessage._1)) {
                  hitRulesJsonMap += (hitRulesMessage._1 -> hitRulesMessage._2)
                }
              })
              hitRulesInfoJson = JsonMapUtils.mapToJson(hitRulesJsonMap)
              hitRulesMessageBuilder.append(hitRulesInfoJson + ";")

            }
            hitRulesJsonInfoBuilder.append(requestInfo + combine + hitRulesMessageBuilder + "\n")

            println("#####################:" + hitRulesJsonInfoBuilder)

          }
        }

      } else {
        hitRulesJsonMap +=("name" -> "-", "uuid" -> "-", "score" -> "-", "decision" -> "-", "id" -> "-", "parentUuid" -> "-")
        val hitRulesOthersInfoJson = JsonMapUtils.mapToJson(hitRulesJsonMap)
        hitRulesJsonInfoBuilder.append(requestInfo + combine + hitRulesOthersInfoJson)
      }
    }
    catch {
      case _ => {
        LOGGER.error("The content hit_rules analyse error!")
      }
    }
    println(hitRulesJsonInfoBuilder)
    return hitRulesJsonInfoBuilder.toString()
  }

  def analyseContentOtherInfoJson(otherJson: JSONObject): String = {
    val otherInfoBuilder = new StringBuilder
    try {
      val policy_set_name = otherJson.get("policy_set_name")
      otherInfoBuilder.append(policy_set_name + "\t")
      val risk_type = otherJson.get("risk_type")
      otherInfoBuilder.append(risk_type + "\t")
      val seq_id = otherJson.get("seq_id")
      otherInfoBuilder.append(seq_id + "\t")
      val spend_time = otherJson.get("spend_time")
      otherInfoBuilder.append(spend_time + "\t")
      val success = otherJson.get("success")
      otherInfoBuilder.append(success + "\t")
      val event_id = otherJson.get("event_id")
      otherInfoBuilder.append(event_id + "\t")
    }
    catch {
      case _ => {
        LOGGER.error("The other json parse other words break failure!")
      }
    }

    return otherInfoBuilder.toString()
  }

}
