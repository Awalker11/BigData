import java.util.{Properties, UUID}


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.parsing.json._

object Main {
  /**
   * 输入的主题名称
   */
  val inputTopic = "mn_buy_ticket_demo2"
  /**
   * kafka地址
   */
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.map {
      x =>
        val j = JSON.parseFull(x) match {
          case Some(map: Map[String, Any]) => map
        }
        (j("destination").toString, 1)
    }.keyBy(_._1).sum(1)
      .keyBy(_._1)
      .timeWindow(Time.seconds(20))
      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          out.collect(elements.maxBy(_._2))

        }
      })
      .timeWindowAll(Time.seconds(20))
      .process(new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          val list = elements.toList
          val sorted = list.sortBy(_._2)
          sorted.reverse.take(5).foreach(println)

        }
      })
    env.execute()
  }
}