package com.badou

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestSession {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = List(
      ("A", 1L, 1),
      ("A", 3L, 1),
      ("A", 5L, 1),

      ("A", 9L, 1),
      ("A", 10L, 1)
    )

    //自定义source
    val source: DataStream[(String, Long, Int)] = senv.addSource(
      new SourceFunction[(String, Long, Int)] {
        override def run(ctx: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {
          input.foreach { value =>
            //通过源直接为每个元素分配一个timestamp：value_2为分配的时间戳
            ctx.collectWithTimestamp(value, value._2)
          }
        }

        override def cancel(): Unit = {}
      }
    )

    val sumData: DataStream[(String, Long, Int)] = source.keyBy(0)
      //3毫秒作为间距，划分session时间间隔（Gap）
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L))).sum(2)

    sumData.print()

    senv.execute("Session Window")
  }
}
