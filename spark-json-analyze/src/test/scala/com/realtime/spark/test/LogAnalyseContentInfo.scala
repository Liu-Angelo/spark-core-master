package com.realtime.spark.test

import com.realtime.spark.utils.JsonMapUtils
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON

/**
 * Describe: 同盾数据解析-解析content的json数据
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object LogAnalyseContentInfo {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  //--- 需要过滤字断 ---
  val filterTongDunData = Map("hit_rules" -> 0)

  /**
   * 同盾数据解析-content解析主要逻辑
   * @param contentInfo
   */
  def analyseContentInfoJson(contentInfo: String): String = {
    val contentBuilder = new StringBuilder
    val json = JSONObject.fromObject(contentInfo)

    //--- content中json数据设备信息(device_info)解析 ---
    val deviceDetailInfo = analyseDeviceInfoJson(json)
    contentBuilder.append(deviceDetailInfo)

    val final_decision = json.get("final_decision")
    contentBuilder.append(final_decision + "\t")
    val final_score = json.get("final_score")
    contentBuilder.append(final_score + "\t")

    //--- 命中规则hitRules解析 ---
    val hit_rules = json.getJSONArray("hit_rules")
    val hitRulesObject = hit_rules.toJSONObject(hit_rules)
    val hitRulesDetailInfo = analyseHitRulesInfoJson(hitRulesObject)
    contentBuilder.append(hitRulesDetailInfo)

    //--- 策略集信息解析 ---
    val policy_name = json.get("policy_name")
    contentBuilder.append(policy_name + "\t")

    val policy_set = json.getJSONArray("policy_set")
    val policySetObject = policy_set.toJSONObject(policy_set)
    val policySetDetailInfo = analysePolicySetInfoJson(policySetObject)
    contentBuilder.append(policySetDetailInfo)

    //--- 其他一级json数据解析 ---
    val otherLevelDetailInfo = analyseContentOtherInfoJson(json)
    contentBuilder.append(otherLevelDetailInfo + "\t")

    return contentBuilder.toString()
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
    device_info match {
      case Some(deviceInfoMap: Map[String, Any]) => {
        deviceInfoMap.keys.foreach(deviceInfoKey => {
          if (deviceInfoKey.contains("geoIp")) {
            val geoIpValueMap = deviceInfoMap.toMap
            geoIpValueMap.keys.foreach(geoIPKey => {
              var geoIPInfo = geoIpValueMap(geoIPKey)
              if (geoIPInfo.equals("") || geoIPInfo == null) {
                geoIPInfo = "-"
                deviceInfoBuilder.append(geoIPInfo + "\t")
              } else {
                deviceInfoBuilder.append(geoIPInfo + "\t")
              }
            })
          }
        })
      }
    }
    return deviceInfoBuilder.toString()
  }

  /**
   * 同盾数据解析-content字段中命中规则信息(hit_rules)解析
   * @param hitRulesObject
   * @return
   */
  def analyseHitRulesInfoJson(hitRulesObject: Object): String = {
    val hitRulesInfoBuilder = new StringBuilder
    val hitRulesInfoMap = JsonMapUtils.jsonToMap(hitRulesObject.toString())
    hitRulesInfoMap.foreach(hitKey => {
      val hitRulesInfo = JSON.parseFull(hitKey._1)
      hitRulesInfo match {
        case Some(hitMap: Map[String, Any]) => {
          hitMap.foreach(rulesKey => {
            val rulesValue = rulesKey._2
            hitRulesInfoBuilder.append(rulesValue + "\t")
          })
        }
        case _ => {
          LOGGER.error("There is no corresponding hit rules data in das data...")
        }
      }
    })
    return hitRulesInfoBuilder.toString()
  }

  /**
   * 同盾数据解析-content字段中命中规则信息(policy_set)解析
   * @param policySetObject
   * @return
   */
  def analysePolicySetInfoJson(policySetObject: Object): String = {
    val policySetBuilder = new StringBuilder
    val policySetInfoMap = JsonMapUtils.jsonToMap(policySetObject.toString())
    policySetInfoMap.foreach(policyKey => {
      val policySetInfo = JSON.parseFull(policyKey._1)
      policySetInfo match {
        case Some(policySetMap: Map[String, Any]) => {
          policySetMap.foreach(policySetKey => {
            if (!filterTongDunData.contains(policySetKey._1)) {
              val policySetValue = policySetKey._2
              policySetBuilder.append(policySetValue + "\t")
            }
          })
        }
        case _ => {
          LOGGER.error("There is no corresponding policy set data in das data...")
        }
      }
    })
    return policySetBuilder.toString()
  }

  /**
   * 同盾数据解析-content中一级json数据解析
   * @param otherJson
   * @return
   */
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
      case _ => LOGGER.error("The other json parse other words break failure!")
    }

    return otherInfoBuilder.toString()
  }

}
