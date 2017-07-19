package com.realtime.spark.config

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/8.
 */
case class Config(
                   consumerGroup: String = "",
                   master: String = "",
                   num_executors: String = "",
                   driver_memory: String = "",
                   executor_memory: String = "",
                   executor_cores: String = "",
                   numThreads: String = ""
                   )
