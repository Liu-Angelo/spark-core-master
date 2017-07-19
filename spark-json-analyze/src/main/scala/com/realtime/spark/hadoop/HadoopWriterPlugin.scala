package com.realtime.spark.hadoop

import com.realtime.spark.utils.DateUtils
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.OutputStreamWriter

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/8.
 */
object HadoopWriterPlugin extends Serializable {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  private val conf = new Configuration()
  private val hdfs = FileSystem.newInstance(conf)
  private val encoding = "UTF-8"

  def writeLine(message: String): Unit = {
    val hdfsOutputFilePath = new Path("/output/risk_das_records/daily/" + DateUtils.transDayFormat() + "/" + "risk_das_records" + "/" + DateUtils.transDayFormat() + ".log")

    def fileExits(): Boolean = hdfs.exists(hdfsOutputFilePath)

    def saveLogDataOutputStream(fsDataOutputStream: FSDataOutputStream): Unit = {
      val outputStreamWriter = new OutputStreamWriter(fsDataOutputStream, encoding)
      val bufferedWriter = new BufferedWriter(outputStreamWriter)

      bufferedWriter.write(s"${message}\n")
      bufferedWriter.close()

      outputStreamWriter.close()
      fsDataOutputStream.close()
    }

    def appendPath(): Unit = {
      val fsDataOutputStream = hdfs.create(hdfsOutputFilePath)
      saveLogDataOutputStream(fsDataOutputStream)
    }

    def create(): Unit = {
      val fsDataOutputStream = hdfs.create(hdfsOutputFilePath)
      saveLogDataOutputStream(fsDataOutputStream)
    }

    if (fileExits()) appendPath() else create()

  }

  def closeFileSystem(): Unit = hdfs.close()
}
