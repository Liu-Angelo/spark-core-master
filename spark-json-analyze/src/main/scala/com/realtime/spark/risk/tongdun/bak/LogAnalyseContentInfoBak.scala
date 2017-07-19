package com.realtime.spark.risk.tongdun.bak

import com.realtime.spark.utils.JsonMapUtils
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON

/**
 * Describe: 同盾数据解析-解析content的json数据
 * Author:   Angelo.Liu
 * Date:     17/2/24.
 */
object LogAnalyseContentInfoBak {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  //--- 需要过滤字断 ---
  val filterTongDunData = Map("hit_rules" -> 0)

  /**
   * 同盾数据解析-content解析主要逻辑
   * @param contentInfo
   */
  def analyseContentInfoJson(contentInfo: String): String = {
    val contentBuilder = new StringBuilder
    val contentJson = JSONObject.fromObject(contentInfo)

    //--- content中json数据设备信息(device_info)解析 ---
    val deviceDetailInfo = analyseDeviceInfoJson(contentJson)
    contentBuilder.append(deviceDetailInfo)

    val final_decision = contentJson.get("final_decision")
    contentBuilder.append(final_decision + "\t")
    val final_score = contentJson.get("final_score")
    contentBuilder.append(final_score + "\t")

    //--- 命中规则hitRules解析 ---
    val hitRulesDetailInfo = analyseHitRulesInfoJson(contentJson)
    contentBuilder.append(hitRulesDetailInfo)

    //--- 策略集信息解析 ---
    val policy_name = contentJson.get("policy_name")
    contentBuilder.append(policy_name + "\t")

    val policySetDetailInfo = analysePolicySetInfoJson(contentJson)
    contentBuilder.append(policySetDetailInfo)

    //--- 其他一级json数据解析 ---
    val otherLevelDetailInfo = analyseContentOtherInfoJson(contentJson)
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
                geoIPInfo = "null"
                deviceInfoBuilder.append(geoIPInfo + "\t")
              } else {
                deviceInfoBuilder.append(geoIPInfo + "\t")
              }
            })
          } else {
            val deviceInfoParam = deviceInfoMap(deviceInfoKey)
            deviceInfoBuilder.append(deviceInfoParam + "\t")
          }
        })
      }
    }
    return deviceInfoBuilder.toString()
  }

  /**
   * 同盾数据解析-content字段中命中规则信息(hit_rules)解析
   * @param contentJson
   * @return
   */
  def analyseHitRulesInfoJson(contentJson: JSONObject): String = {
    val hitRulesInfoBuilder = new StringBuilder
    try {
      var hit_rules = ""
      if (!contentJson.containsKey("hit_rules")) {
        hit_rules = "null"
        hitRulesInfoBuilder.append(hit_rules + "\t")
      } else if (contentJson.containsKey("hit_rules")) {
        val hit_rulesInfo = contentJson.getJSONArray("hit_rules")
        val hitRulesObject = hit_rulesInfo.toJSONObject(hit_rulesInfo)
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
      } else {
        hit_rules = "null"
        hitRulesInfoBuilder.append(hit_rules + "\t")
      }
    }
    catch {
      case _ => {
        LOGGER.error("The content hit_rules analyse error!")
      }
    }

    return hitRulesInfoBuilder.toString()
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
      val policySetObject = policy_setInfo.toJSONObject(policy_setInfo)
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
    } else {
      policy_set = "null"
      policySetBuilder.append(policy_set + "\t")
    }

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
      case _ => {
        LOGGER.error("The other json parse other words break failure!")
      }
    }

    return otherInfoBuilder.toString()
  }

}
