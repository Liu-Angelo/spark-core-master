package com.realtime.spark.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.slf4j.LoggerFactory

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/8.
 */
object DateUtils {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  private val DATE_FORMAT_TO_DAY: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  private val DATE_FORMAT_TO_HOUR: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def transDayFormat(): String = {
    val date_time: Date = new Date()
    val current_day = DATE_FORMAT_TO_DAY.format(date_time)
    current_day
  }


  def transHourFormat(): String = {
    val now_time: Date = new Date()
    val current_hour = DATE_FORMAT_TO_HOUR.format(now_time)
    current_hour
  }

  private def formatData(line: Date) = {
    val date = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
    val dateFormated = date.format(line)
    val dateFf = date.parse(dateFormated).getTime
    dateFf
  }

  //--- 获取昨日时间 ---
  def getYesterday(): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

}
