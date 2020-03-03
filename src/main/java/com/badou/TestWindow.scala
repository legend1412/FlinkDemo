package com.badou

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object TestWindow {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data = senv.socketTextStream("192.168.137.3",9999)
    //val data = senv.socketTextStream("192.168.110.110", 9999)
    val stream = data.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
        //定义timestamp怎么ongoing数据中抽取出来[1583209245000 a 2020-03-03 12:20:45]
        override def extractTimestamp(t: String): Long = {
          val eventTime = t.split(" ")(0).toLong
          val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val tim = fm.format(new Date(eventTime.toLong))
          println(s"时间戳:$eventTime"+"\t时间:"+tim)
          eventTime
        }
      }).map(line => (line.split(" ")(1), 1L)).keyBy(0)

    //翻滚的时间区间前闭后开，跟水位线需要结合起来计算，水位线虽然是5秒，但是窗口是3秒一输出，所以要输出某个数据，
    // 要看这个数据在的这个窗口的临界值加上5秒才行，其实可以理解为时窗口的临界值等了5秒，而不是每个数据等了5秒
    stream
      //按照事件时间的滑动
      //  .window(SlidingEventTimeWindows.of(Time.seconds(3),Time.seconds(1))).sum(1).print()
    //      //按照事件时间的翻滚
      //.window(TumblingEventTimeWindows.of(Time.seconds(3))).sum(1).print()
    //    data.map((_,1L)).keyBy(0)
    //      //session window
          .window(EventTimeSessionWindows.withGap(Time.seconds(3))).sum(1).print()
    //      //事件窗口，按照事件数量（针对具体的key进行统计的数量）进行输出
    //      //.countWindow(5)
    //      //只有一个时间的时候是翻滚窗口timeWindow(Time.seconds(3))
    //      //有两个时间的时候是滑动窗口timeWindow(Time.seconds(3),Time.seconds(1))
    //      //.timeWindow(Time.seconds(3),Time.seconds(1))
    //      .sum(1).print()

    senv.execute("Test Windows")
  }
}
