import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.immutable.HashMap
import scala.util.parsing.json.{JSON, JSONObject}

object ReadMysqlToKafka {
  val topic = "TransferData"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    import java.sql.DriverManager
    val url = "jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user22_db"
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("user", "user22")
    properties.setProperty("password", "pass@bingo22")
    val connection = DriverManager.getConnection(url, properties)
    val statement = connection.createStatement
    val resultSet = statement.executeQuery("select * from t_rk_jbxx_result;")

    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    try {
      while(resultSet.next) {
        var show = new HashMap[String, String]
        show+=("sfzhm"->resultSet.getString(1))
        show+=("xm"->resultSet.getString(2))
        show+=("asjbh"->resultSet.getString(3))
        show+=("ajmc"->resultSet.getString(4))
        show+=("aj_jyqk"->resultSet.getString(5))
        val s = JSONObject(show).toString().trim
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据："+s)
        producer.send(record)
      }
      producer.flush()
      producer.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
