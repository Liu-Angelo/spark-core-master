package com.realtime.spark.utils

import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/10.
 */
object JsonMapUtils {

  /**
   * 将map转成json
   */
  def mapToJson(map: Map[String, Object]): String = {
    val jsonString = JSONObject.toJSONString(map)
    jsonString
  }

  /**
   * 将json转为map
   * @param json 输入json字符串
   * @return
   *
   */
  def jsonToMap(json: String): mutable.HashMap[String, Object] = {
    val map: mutable.HashMap[String, Object] = mutable.HashMap()

    val jsonParser = new JSONParser()

    //将String转化为jsonObject
    val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]

    //获取所有键值
    val jsonKey = jsonObj.keySet()

    val iter = jsonKey.iterator()

    while (iter.hasNext) {
      val field = iter.next()
      val value = jsonObj.get(field).toString

      if (value.startsWith("{") && value.endsWith("}")) {
        val value = mapAsScalaMap(jsonObj.get(field).asInstanceOf[util.HashMap[String, String]])
        map.put(field, value)
      } else {
        map.put(field, value)
      }
    }
    map
  }


  /**
   * 判断字符串是不是数字
   */
  def isIntByRegex(s: String): String = {
    val pattern = """^(\d+)$""".r
    s match {
      case pattern(_*) => s
      case _ => "-"
    }
  }

}
