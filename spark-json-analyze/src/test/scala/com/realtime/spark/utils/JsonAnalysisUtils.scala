package com.realtime.spark.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON

import net.sf.json.JSONObject

import org.slf4j.LoggerFactory
import play.api.libs.json

import scala.collection.immutable.HashMap

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/10.
 */
object JsonAnalysisUtils {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val json_str = "{\n    \"device_info\": {\n        \"deviceType\": \"Android\",\n        \"screenRes\": \"1080^^1920^^-^^-\",\n        \"languageRes\": \"zh-CN^^-^^-^^zh-CN^^-\",\n        \"referer\": \"https://lfq.qufenqi.com/i/iou/29?channelFrom=srv&referer=srvmenu1_1&v=2026\",\n        \"canvas\": \"3a9e28207b0ebdd437ca4ab2cce60071\",\n        \"cookieEnabled\": \"true\",\n        \"geoIp\": {\n            \"address\": \"\",\n            \"area\": \"\",\n            \"areaId\": \"\",\n            \"city\": \"杭州市\",\n            \"cityId\": \"330100\",\n            \"country\": \"中国\",\n            \"countryId\": \"CN\",\n            \"county\": \"\",\n            \"countyId\": \"330100\",\n            \"desc\": \"\",\n            \"extra1\": \"\",\n            \"extra2\": \"\",\n            \"ip\": \"110.75.152.1\",\n            \"isp\": \"阿里云/电信/联通/移动/铁通/教育网\",\n            \"ispId\": \"\",\n            \"latitude\": 30.287458,\n            \"lip\": 1850447873,\n            \"longitude\": 120.15358,\n            \"province\": \"浙江省\",\n            \"provinceId\": \"330000\",\n            \"type\": \"\"\n        },\n        \"tokenId\": \"71ae66796e81d0d8cb2b13da903bd0bb\",\n        \"os\": \"android\",\n        \"flashEnabled\": \"false\",\n        \"userAgent\": \"Mozilla/5.0 (Linux; U; Android 6.0.1; zh-cn; OPPO R9s Build/MMB29M) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/1.0.0.100 U3/0.8.0 Mobile Safari/534.30 Nebula AlipayDefined(nt:WIFI,ws:360|640|3.0) AliApp(AP/10.0.3.021302) AlipayClient/10.0.3.021302 Language/zh-Hans useStatusBar/true\",\n        \"version\": \"0.0.3\",\n        \"deviceId\": \"f747009b24d6a868bd7e8a645414f1a3\",\n        \"browserType\": \"safari\",\n        \"fontId\": \"-\",\n        \"success\": true,\n        \"browser\": \"safari\",\n        \"browserVersion\": \"534.30\",\n        \"trueIp\": \"110.75.152.1\",\n        \"pluginList\": \"0,-\",\n        \"smartId\": \"g_32e5c6286b0fc823bb552e8313634641\"\n    },\n    \"final_decision\": \"Review\",\n    \"final_score\": 31,\n    \"hit_rules\": [\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564336\",\n            \"name\": \"使用HTTP代理进行借款\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"6e74bae314054a8987cce0df23b2c59f\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564346\",\n            \"name\": \"不在手机归属地城市借款\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"4b53485d70234623bd031916275a1614\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"564348\",\n            \"name\": \"借款IP与真实IP的城市不匹配\",\n            \"parentUuid\": \"\",\n            \"score\": 0,\n            \"uuid\": \"50de1c6d84354b84baa296f8b9aa0e66\"\n        },\n        {\n            \"decision\": \"Accept\",\n            \"id\": \"1336828\",\n            \"name\": \"3个月内申请人信息多平台借款\",\n            \"parentUuid\": \"\",\n            \"score\": 31,\n            \"uuid\": \"7a5e6b8889cb432eafe6c347c42d96ce\"\n        }\n    ],\n    \"policy_name\": \"借款策略集\",\n    \"policy_set\": [\n        {\n            \"hit_rules\": [\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564336\",\n                    \"name\": \"使用HTTP代理进行借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"6e74bae314054a8987cce0df23b2c59f\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564346\",\n                    \"name\": \"不在手机归属地城市借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"4b53485d70234623bd031916275a1614\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"564348\",\n                    \"name\": \"借款IP与真实IP的城市不匹配\",\n                    \"parentUuid\": \"\",\n                    \"score\": 0,\n                    \"uuid\": \"50de1c6d84354b84baa296f8b9aa0e66\"\n                },\n                {\n                    \"decision\": \"Accept\",\n                    \"id\": \"1336828\",\n                    \"name\": \"3个月内申请人信息多平台借款\",\n                    \"parentUuid\": \"\",\n                    \"score\": 31,\n                    \"uuid\": \"7a5e6b8889cb432eafe6c347c42d96ce\"\n                }\n            ],\n            \"policy_decision\": \"Review\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"异常借款\",\n            \"policy_score\": 31,\n            \"policy_uuid\": \"d77e0c204c0849b9acbe47697a964b4d\",\n            \"risk_type\": \"suspiciousLoan\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"Weighted\",\n            \"policy_name\": \"机构代办\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"bf47f5fe4c3d4f69bbed15771e5b1497\",\n            \"risk_type\": \"agency\"\n        },\n        {\n            \"policy_decision\": \"Accept\",\n            \"policy_mode\": \"WorstMatch\",\n            \"policy_name\": \"失信借款\",\n            \"policy_score\": 0,\n            \"policy_uuid\": \"261b115400d94f4b9af8735c39137a0e\",\n            \"risk_type\": \"creditRisk\"\n        }\n    ],\n    \"policy_set_name\": \"借款策略集\",\n    \"risk_type\": \"suspiciousLoan_review\",\n    \"seq_id\": \"1487636664651843S180E120F5634028\",\n    \"spend_time\": 39,\n    \"success\": true,\n    \"event_id\": \"loan_web\"\n}"
    //     tongdunJson(json_str)

  }


  /**
   * json-lib处理json
   *
   * @param json_str
   */
  def jsonLib(json_str: String): Unit = {
    val data = JSONObject.fromObject(json_str)
    //--- 获取json元素成员 ---］
    val biz_no = data.get("biz_no")
    //获取整型数据
    val vtm = data.getInt("vtm")

    //获取多级元素
    val vars = data.getJSONObject("vars")
    val company_name = vars.getString("company_name")
    val ovd_order_cnt_2y_m6_status = vars.getString("ovd_order_cnt_2y_m6_status")

    println("====:" + vars + "\n" + "***:" + biz_no + "****:" + vtm + "***:" + company_name + "***:" + ovd_order_cnt_2y_m6_status)
  }

  /**
   * fastjson处理json
   */
  def fastJson(json_str: String): Unit = {
    val json = JSON.parseObject(json_str)
    //--- 获取json成员元素 ---
    val success = json.get("success")
    //--- 返回字符串成员元素 ---
    val success_str = json.getString("success")

    //--- 返回整型成员 ---
    val vtm = json.getString("vtm")
    //--- 返回多级成员元素 ---
    val vars = json.getJSONObject("vars")
    val company_name = vars.get("company_name")
    val occupation = vars.get("occupation")
    println("@@@@@@@:" + vars + "\n" + "----:" + success_str + "----:" + vtm + "----:" + company_name + "----:" + occupation)

  }




}
