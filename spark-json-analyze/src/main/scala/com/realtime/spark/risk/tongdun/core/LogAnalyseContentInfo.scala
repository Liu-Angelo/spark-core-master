package com.realtime.spark.risk.tongdun.core

import com.realtime.spark.utils.JsonMapUtils
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: 同盾数据解析-解析content的json数据
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object LogAnalyseContentInfo {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val filterHitRulesField = Map("uuid" -> 0, "score" -> 0, "decision" -> 0, "parentUuid" -> 0)

  /**
   * 同盾数据解析-content解析主要逻辑
   * @param contentInfo
   */
  def analyseContentInfoJson(extraFieldInfo: String, requestInfo: String, contentInfo: String, timeInfo: String): String = {
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

    //--- 其他一级json数据解析 ---
    val otherLevelDetailInfo = analyseContentOtherInfoJson(contentJson)
    contentBuilder.append(otherLevelDetailInfo)

    val requestAnalyseDetailInfo = LogAnalyseRequestInfo.analyseRequestInfoJson(requestInfo)

    //--- 命中规则信息解析 ---
    val hitRulesDetailInfo = analyseHitRulesInfoJson(extraFieldInfo, requestAnalyseDetailInfo, contentBuilder.toString(), contentJson, timeInfo)

    val requestAndContentInfo = hitRulesDetailInfo.toString

    return requestAndContentInfo

  }

  /**
   * 同盾数据解析-content字段中设备信息(deviceInfo)解析
   * @param deviceInfo
   * @return
   */
  def analyseDeviceInfoJson(deviceInfo: JSONObject): String = {
    val deviceInfoBuilder = new StringBuilder
    var geoIPInfoMap = new HashMap[String, Object]

    val device = deviceInfo.get("device_info")
    val device_info = JSON.parseFull(device.toString)
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
   * 同盾数据解析-content字段中命中策略集中的规则信息(hit_rules)解析
   * @param contentJson
   * @return
   */
  def analyseHitRulesInfoJson(extraFieldInfo: String, requestInfo: Object, combine: String, contentJson: JSONObject, timeInfo: String): String = {
    val hitRulesJsonInfoBuilder = new StringBuilder
    val hitRulesMessageBuilder = new StringBuilder

    var hitRulesJsonMap = new HashMap[String, Object]
    try {
      if (!contentJson.containsKey("hit_rules")) {
        hitRulesJsonMap +=("name" -> "-", "id" -> "-")
        val hitRulesNullInfoJson = JsonMapUtils.mapToJson(hitRulesJsonMap)
        hitRulesJsonInfoBuilder.append(extraFieldInfo + requestInfo + combine + hitRulesNullInfoJson + "\t" + timeInfo)
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
            hitRulesJsonInfoBuilder.append(extraFieldInfo + requestInfo + combine + hitRulesMessageBuilder + "\t" + timeInfo)
          }
        }

      } else {
        hitRulesJsonMap +=("name" -> "-", "id" -> "-")
        val hitRulesOthersInfoJson = JsonMapUtils.mapToJson(hitRulesJsonMap)
        hitRulesJsonInfoBuilder.append(extraFieldInfo + requestInfo + combine + hitRulesOthersInfoJson + "\t" + timeInfo)
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
