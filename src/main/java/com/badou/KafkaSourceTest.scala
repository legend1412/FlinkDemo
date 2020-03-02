package com.badou

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

object KafkaSourceTest {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(211111)
    //senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.137.3:9092")
    properties.setProperty("zookeeper.connect","192.168.137.3:2181")
    properties.setProperty("group.id","test_flink")

    val stream = senv.addSource(new FlinkKafkaConsumer08[String]("badou",new SimpleStringSchema(),properties))
    stream.setParallelism(10).flatMap(_.split(" ")).map((_,1L)).keyBy(0).sum(1).print()
    senv.execute("Kafka Source")
  }
}
