import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import java.util.{Properties, ArrayList}
import java.nio.charset.StandardCharsets
import org.apache.flink.api.common.serialization.DeserializationSchema
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scalaj.http.Http
import org.everit.json.schema.{Schema, ValidationException}
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

// Định nghĩa Order
case class Order(customer_id: String, amount: Double, order_date: String)

// Deserialization và validate schema
class OrderDeserializationSchema(expectedVersion: String) extends DeserializationSchema[Order] {

  @transient private lazy val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  @transient private var schemaValidator: Schema = _

  // Lấy schema từ registry và tạo validator
  private def loadSchemaValidator(version: String): Unit = {
    try {
      val response = Http(s"http://localhost:8081/subjects/orders2-value/versions/$version").asString
      if (response.isError) {
        println(s"Error fetching schema: ${response.body}")
        return
      }
      val schemaNode = objectMapper.readTree(response.body).get("schema")
      val schemaObject = new JSONObject(schemaNode.asText())
      schemaValidator = SchemaLoader.load(schemaObject)
      println("Schema loaded and validator initialized.")
    } catch {
      case e: Exception =>
        println(s"Failed to load schema from registry: ${e.getMessage}")
    }
  }

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    loadSchemaValidator(expectedVersion)
  }

  override def deserialize(message: Array[Byte]): Order = {
    val filteredMessage = message.dropWhile(b => b == 0 || b == 1)
    val json = new String(filteredMessage, StandardCharsets.UTF_8)
    println(s"Raw JSON from Kafka: $json")

    val messageNode = objectMapper.readTree(json)

    if (schemaValidator == null) {
      println("Warning: Schema validator is null. Skipping validation.")
    } else {
      try {
        schemaValidator.validate(new JSONObject(messageNode.toString))
        println("JSON message is valid.")
      } catch {
        case e: ValidationException =>
          println(s"Schema validation failed: ${e.getMessage}")
          println("Skipping invalid record but continuing job...")
          return null  // Trả về null để bỏ qua bản ghi
      }
    }
    objectMapper.readValue(json, classOf[Order])
  }


  override def isEndOfStream(nextElement: Order): Boolean = false

  override def getProducedType: TypeInformation[Order] = createTypeInformation[Order]
}

// Kafka Consumer Flink
object KafkaFlinkConsumerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-consumer-group")

    // Xác định schema version mong muốn
    val expectedSchemaVersion = "1"

    println(s"Fetching schema for subject: orders2-value, version: $expectedSchemaVersion...")

    val response = Http(s"http://localhost:8081/subjects/orders2-value/versions/$expectedSchemaVersion").asString

    if (response.isError) {
      println(s"Failed to fetch schema: ${response.body}")
      return
    }

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val schemaNode = objectMapper.readTree(response.body)

    println("Fetched Schema from Registry:")
    println(schemaNode.toPrettyString())

    // Buffer để gom batch
    val batchBuffer = ListBuffer[Order]()

    // Tạo Kafka Consumer
    val kafkaConsumer = new FlinkKafkaConsumer[Order](
      "orders2",
      new OrderDeserializationSchema(expectedSchemaVersion),
      properties
    )

    val stream = env.addSource(kafkaConsumer)

    // Thu thập và xử lý batch
    stream.map { order =>
      batchBuffer.append(order)
      println(s"Order added to batch: $order")

      if (batchBuffer.size >= 5) {
        println(s"Sending batch of size ${batchBuffer.size} to API...")
        sendBatchToAPI(batchBuffer.toList)
        batchBuffer.clear()
      }

      order
    }.name("Batch Processor")

    env.execute("Flink Kafka JSON Schema Consumer with Batch API")
  }

    // Hàm gửi batch đến API Flask (di chuyển vào trong object)
  def sendBatchToAPI(batch: List[Order]): Unit = {
    try {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)  // Đảm bảo Jackson hỗ trợ Scala case class

      val jsonBatch = objectMapper.writeValueAsString(batch)

      val response = Http("http://localhost:8080/batch")
        .postData(jsonBatch)
        .header("Content-Type", "application/json")
        .asString

      if (response.is2xx) {
        println(s"Batch sent successfully: ${response.body}")
      } else {
        println(s"Failed to send batch: ${response.body}")
      }
    } catch {
      case e: Exception => println(s"Error sending batch to API: ${e.getMessage}")
    }
  }

}
