package com.badou

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object TestWindow {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.137.3",9999)

    data.map((_,1)).keyBy(0)
      //按照事件时间的翻滚
      .window(TumblingEventTimeWindows)
      //.window(SlidingEventTimeWindows)
      //session window
      //.window(EventTimeSessionWindows.withGap(Time.seconds(3)))
      //事件窗口，按照事件数量（针对具体的key进行统计的数量）进行输出
      //.countWindow(5)
      //只有一个时间的时候是翻滚窗口timeWindow(Time.seconds(3))
      //有两个时间的时候是滑动窗口timeWindow(Time.seconds(3),Time.seconds(1))
      //.timeWindow(Time.seconds(3),Time.seconds(1))
      .sum(1).print()

    senv.execute("Test Windows")
  }
}
