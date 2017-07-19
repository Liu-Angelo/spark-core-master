package com.realtime.spark.utils

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import net.sf.json.JSONObject
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsValue}

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/23.
 */
object TongdunJsonInfoBak {
  val filterTongDunData = Map("hit_rules" -> 0)

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val content_json = "{\n    \"device_info\": {\n        \"deviceType\": \"Android\",\n        \"screenRes\": \"1080^^1920^^-^^-\",\n        \"languageRes\": \"zh-CN^^-^^-^^zh-CN^^-\",\n        \"referer\": \"https://lfq.qufenqi.com/i/iou/29?channelFrom=srv&referer=srvmenu1_1&v=2026\",\n        \"canvas\": \"3a9e28207b0ebdd437ca4ab2cce60071\",\n        \"cookieEnabled\": \"true\",\n        \"geoIp\": {\n            \"address\": \"\",\n            \"area\": \"\",\n            \"areaId\": \"\",\n            \"city\": \"杭州市\",\n            \"cityId\": \"330100\",\n            \"country\": \"中国\",\n            \"countryId\": \"CN\",\n            \"county\": \"\",\n            \"countyId\": \"330100\",\n            \"desc\": \"\",\n            \"extra1\": \"\",\n            \"extra2\": \"\",\n            \"ip\": \"110.75.152.1\",\n            \"isp\": \"阿里云/电信/联通/移动/铁通/教育网\",\n            \"ispId\": \"\",\n            \"latitude\": 30.287458,\n            \"lip\": 1850447873,\n            \"longitude\": 120.15358,\n            \"province\": \"浙江省\",\n            \"provinceId\": \"330000\",\n            \"type\": \"\"\n        },\n        \"tokenId\": \"71ae66796e81d0d8cb2b13da903bd0bb\",\n        \"os\": \"android\",\n        \"flashEnabled\": \"false\",\n        \"userAgent\": \"Mozilla/5.0 (Linux; U; Android 6.0.1; zh-cn; OPPO R9s Build/MMB29M) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/1.0.0.100 U3/0.8.0 Mobile Safari/534.30 Nebula AlipayDefined(nt:WIFI,ws:360|640|3.0) AliApp(AP/10.0.3.021302) AlipayClient/10.0.3.021302 Language/zh-Hans useStatusBar/true\",\n        \"version\": \"0.0.3\",\n        \"deviceId\": \"f747009b24d6a868bd7e8a645414f1a3\",\n        \"browserType\": \"safari\",\n        \"fontId\": \"-\",\n        \"success\": true,\n        \"browser\": \"safari\",\n        \"browserVersion\": \"534.30\",\n        \"trueIp\": \"110.75.152.1\",\n        \"pluginList\": \"0,-\",\n        \"smartId\": \"g_32e5c6286b0fc823bb552e8313634641\"\n    },\n    \"final_decision\": \"Review\",\n    \"final_score\": 31,\n    \"hit_rules\": [\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564336\",\n            \"name\": \"使用HTTP代理进行借款\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"6e74bae314054a8987cce0df23b2c59f\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564346\",\n            \"name\": \"不在手机归属地城市借款\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"4b53485d70234623bd031916275a1614\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564348\",\n            \"name\": \"借款IP与真实IP的城市不匹配\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"50de1c6d84354b84baa296f8b9aa0e66\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"1336828\",\n            \"name\": \"3个月内申请人信息多平台借款\",\n            \"parentUuid\": \"\",\n            \"score\": 31,\n            \"uuid\": \"7a5e6b8889cb432eafe6c347c42d96ce\"\n        }\n    ],\n    \"policy_name\": \"借款策略集\",\n    \"policy_set\": [\n        {\n            \"hit_rules\": [\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564336\",\n                    \"name\": \"使用HTTP代理进行借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"6e74bae314054a8987cce0df23b2c59f\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564346\",\n                    \"name\": \"不在手机归属地城市借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"4b53485d70234623bd031916275a1614\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564348\",\n                    \"name\": \"借款IP与真实IP的城市不匹配\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"50de1c6d84354b84baa296f8b9aa0e66\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"1336828\",\n                    \"name\": \"3个月内申请人信息多平台借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 31,\n                    \"uuid\": \"7a5e6b8889cb432eafe6c347c42d96ce\"\n                }\n            ],\n            \"policy_decision\": \"Review\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"异常借款\",\n            \"policy_score\": 31,\n            \"policy_uuid\": \"d77e0c204c0849b9acbe47697a964b4d\",\n            \"risk_type\": \"suspiciousLoan\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"机构代办\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"bf47f5fe4c3d4f69bbed15771e5b1497\",\n            \"risk_type\": \"agency\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"WorstMatch\",\n            \"policy_name\": \"失信借款\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"261b115400d94f4b9af8735c39137a0e\",\n            \"risk_type\": \"creditRisk\"\n        }\n    ],\n    \"policy_set_name\": \"借款策略集\",\n    \"risk_type\": \"suspiciousLoan_review\",\n    \"seq_id\": \"1487636664651843S180E120F5634028\",\n    \"spend_time\": 39,\n    \"success\": true,\n    \"event_id\": \"loan_web\"\n}"
    //    analyseContentInfoJson(content_json)

    val request_json = "{\"user_id\":\"22428398\",\"name\":\"吴鸿亮\",\"user_name\":\"吴鸿亮\",\"registed_mobile\":\"15872213351\",\"mobile\":\"15872213351\",\"id_number\":\"420602199801182516\",\"customer_id\":\"268813897533674050765156280\",\"user_status\":{\"id\":\"29234496\",\"user_id\":\"20881049666252685831227771012142\",\"alipay_user_id\":\"2088212102663425\",\"cert_no\":\"420602199801182516\",\"real_name\":\"吴鸿亮\",\"avatar\":\"https:\\/\\/tfs.alipayobjects.com\\/images\\/partner\\/T1OSdeXXNkXXXXXXXX\",\"mobile\":\"15872213351\",\"gender\":\"m\",\"user_status\":\"T\",\"user_type_value\":\"2\",\"is_id_auth\":\"T\",\"is_mobile_auth\":\"T\",\"is_bank_auth\":\"T\",\"is_student_certified\":\"F\",\"is_certify_grade_a\":\"T\",\"is_certified\":\"T\",\"is_licence_auth\":\"F\",\"cert_type_value\":\"0\",\"lfq_user_id\":\"22428398\",\"customer_id\":null,\"score\":null,\"alipay_account\":\"2088212102663425\",\"created_at\":\"2017-01-24 19:35:07\",\"updated_at\":\"2017-01-24 19:35:28\",\"deleted_at\":null,\"app_id\":\"1\",\"account_id\":\"0\",\"order_num\":1,\"hasPaid\":1,\"payed_bill_num\":3,\"max_overdue\":0,\"overdue_status\":\"F\",\"redo_bill_num\":0,\"total_overdue_days\":0,\"overdue_times\":0},\"ip_address\":\"183.207.217.99\",\"is_test\":0,\"registed_at\":\"2017-01-24 19:35:26\",\"user_type\":\"normal\",\"order_type\":\"qudian\",\"iou_limit\":\"700.00\",\"audit_limit\":\"2800.00\",\"iou_amount\":\"250.01\",\"audit_amount\":\"250.01\",\"alipay_user_id\":\"2088212102663425\",\"loan_money\":\"400\",\"loan_term\":\"6\",\"loan_fenqi\":\"week\",\"token_id\":\"67a46b4a70f1db72686f0575ac6faee4\",\"loan_type\":1,\"execute_rate\":1,\"addr_id\":[\"65858762\"],\"is_temp_limit\":\"0\",\"user_address\":[{\"id\":\"65858762\",\"prov\":\"江苏省\",\"city\":\"南通市\",\"area\":\"如皋市\",\"mobile\":\"15997181648\",\"name\":\"吴鸿亮\",\"address\":\"长江镇红星北区一二一栋，一零一室\"}],\"random_weight\":698,\"partner_id\":\"10003\"}\t"
    analyseRequestInfoJson(request_json)
  }

  def analyseContentInfoJson(contentInfo: String): Unit = {
    val contentBuilder = new StringBuilder
    val json = JSONObject.fromObject(contentInfo)
    //--- 设备信息deviceInfo解析 ---
    val device_info = json.getJSONObject("device_info")
    val deviceInfoMap = JsonMapUtils.jsonToMap(device_info.toString())
    deviceInfoMap.foreach(key => {
      if (key._1.contains("geoIp")) {
        println("88888888888:" + key._2)
        contentBuilder.append(key._2 + "\t")
      } else {
        contentBuilder.append(key._2 + "\t")
      }
    })

    val final_decision = json.get("final_decision")
    contentBuilder.append(final_decision + "\t")
    val final_score = json.get("final_score")
    contentBuilder.append(final_score + "\t")

    //--- 命中规则hitRules解析 ---
    val hit_rules = json.getJSONArray("hit_rules")
    val hitRulesObject = hit_rules.toJSONObject(hit_rules)
    val hitRulesInfoMap = JsonMapUtils.jsonToMap(hitRulesObject.toString())
    hitRulesInfoMap.foreach(hitKey => {
      val hitRulesInfo = JSON.parseFull(hitKey._1)
      hitRulesInfo match {
        case Some(hitMap: Map[String, Any]) => {
          hitMap.foreach(rulesKey => {
            val rulesValue = rulesKey._2
            contentBuilder.append(rulesValue + "\t")
          })
        }
        case _ => {
          LOGGER.error("There is no corresponding hit rules data in das data...")
        }
      }
    })
    //--- 策略集信息解析 ---
    val policy_name = json.get("policy_name")
    val policy_set = json.getJSONArray("policy_set")
    val policySetObject = policy_set.toJSONObject(policy_set)
    val policySetInfoMap = JsonMapUtils.jsonToMap(policySetObject.toString())
    policySetInfoMap.foreach(policyKey => {
      val policySetInfo = JSON.parseFull(policyKey._1)
      policySetInfo match {
        case Some(policySetMap: Map[String, Any]) => {
          policySetMap.foreach(policySetKey => {
            if (!filterTongDunData.contains(policySetKey._1)) {
              val policySetValue = policySetKey._2
              contentBuilder.append(policySetValue + "\t")
            }
          })
        }
        case _ => {
          LOGGER.error("There is no corresponding policy set data in das data...")
        }
      }
    })

    val policy_set_name = json.get("policy_set_name")
    contentBuilder.append(policy_set_name + "\t")
    val risk_type = json.get("risk_type")
    contentBuilder.append(risk_type + "\t")
    val seq_id = json.get("seq_id")
    contentBuilder.append(seq_id + "\t")
    val spend_time = json.get("spend_time")
    contentBuilder.append(spend_time + "\t")
    val success = json.get("success")
    contentBuilder.append(success + "\t")
    val event_id = json.get("event_id")
    contentBuilder.append(event_id + "\t")

    println("------------:" + contentBuilder.toString())
  }

  def analyseRequestInfoJson(requestInfo: String): Unit = {
    val requestBuilder = new StringBuilder
    val requestJson = JSONObject.fromObject(requestInfo)
    val user_id = requestJson.get("user_id")
    requestBuilder.append(user_id + "\t")
    val name = requestJson.get("name")
    requestBuilder.append(name + "\t")
    val user_name = requestJson.get("user_name")
    requestBuilder.append(user_name + "\t")
    val registed_mobile = requestJson.get("registed_mobile")
    requestBuilder.append(registed_mobile + "\t")
    val mobile = requestJson.get("mobile")
    requestBuilder.append(mobile + "\t")
    val id_number = requestJson.get("id_number")
    requestBuilder.append(id_number + "\t")
    val customer_id = requestJson.get("customer_id")
    requestBuilder.append(customer_id + "\t")
    val user_status_info = requestJson.get("user_status")
    val userStatusInfoMap = JSON.parseFull(user_status_info.toString)
    userStatusInfoMap match {
      case Some(userInfoMap: Map[String, Any]) => {
        userInfoMap.foreach(userInfoKey => {
          val userInfoValue = userInfoKey._2
          requestBuilder.append(userInfoValue + "\t")
        })
      }
      case _ => {
        LOGGER.error("The Request in the json data parsing user_status data error!")
      }
    }
    val ip_address = requestJson.get("ip_address")
    requestBuilder.append(ip_address + "\t")
    val is_test = requestJson.get("is_test")
    requestBuilder.append(is_test + "\t")
    val registed_at = requestJson.get("registed_at")
    requestBuilder.append(registed_at + "\t")
    val user_type = requestJson.get("user_type")
    requestBuilder.append(user_type + "\t")
    val order_type = requestJson.get("order_type")
    requestBuilder.append(order_type + "\t")
    val iou_limit = requestJson.get("iou_limit")
    requestBuilder.append(iou_limit + "\t")
    val audit_limit = requestJson.get("audit_limit")
    requestBuilder.append(audit_limit + "\t")
    val iou_amount = requestJson.get("iou_amount")
    requestBuilder.append(iou_amount + "\t")
    val audit_amount = requestJson.get("audit_amount")
    requestBuilder.append(audit_amount + "\t")
    val alipay_user_id = requestJson.get("alipay_user_id")
    requestBuilder.append(alipay_user_id + "\t")
    val loan_money = requestJson.get("loan_money")
    requestBuilder.append(loan_money + "\t")
    val loan_term = requestJson.get("loan_term")
    requestBuilder.append(loan_term + "\t")
    val loan_fenqi = requestJson.get("loan_fenqi")
    requestBuilder.append(loan_fenqi + "\t")
    val token_id = requestJson.get("token_id")
    requestBuilder.append(token_id + "\t")
    val loan_type = requestJson.get("loan_type")
    requestBuilder.append(loan_type + "\t")
    val execute_rate = requestJson.get("execute_rate")
    requestBuilder.append(execute_rate + "\t")

    val addr_id = requestJson.getJSONArray("addr_id")
    val addrId = addr_id.toJSONObject(addr_id)
    val addIdMap = JsonMapUtils.jsonToMap(addrId.toString())
    addIdMap.keys.foreach(addIdKey => {
      val addIdValue = addIdMap(addIdKey)
      requestBuilder.append(addIdValue + "\t")
    })

    val is_temp_limit = requestJson.get("is_temp_limit")
    requestBuilder.append(is_temp_limit + "\t")

    val user_address = requestJson.getJSONArray("user_address")
    val userAddressObject = user_address.toJSONObject(user_address)
    val userAddressInfoMap = JsonMapUtils.jsonToMap(userAddressObject.toString())
    userAddressInfoMap.foreach(userAddressKey => {
      val userAddrInfo = JSON.parseFull(userAddressKey._1)
      userAddrInfo match {
        case Some(userAddrValueMap: Map[String, Any]) => {
          userAddrValueMap.keys.foreach(userAddrInfoKey => {
            val userAddrInfoValue = userAddrValueMap(userAddrInfoKey)
            requestBuilder.append(userAddrInfoValue + "\t")
          })
        }
        case _ => {
          LOGGER.error("The Request in the json data parsing user_address data error!")
        }
      }
    })

    val random_weight = requestJson.get("random_weight")
    requestBuilder.append(random_weight + "\t")
    val partner_id = requestJson.get("partner_id")
    requestBuilder.append(partner_id + "\t")
    println("*************:" + requestBuilder.toString())


  }

}
