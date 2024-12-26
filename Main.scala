import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

// HTTP Client Imports
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._

/**
  * Avro Deserialization Schema with Schema Validation
  */
class AvroDeserializationSchemaMultiple(schemaRegistryUrl: String, allowedSchemaIds: Set[Int])
  extends DeserializationSchema[GenericRecord] {

  @transient lazy val schemaRegistryClient: SchemaRegistryClient =
    new CachedSchemaRegistryClient(schemaRegistryUrl, 10)

  override def deserialize(message: Array[Byte]): GenericRecord = {
    val magicByte = message(0)
    if (magicByte != 0) {
      throw new RuntimeException(s"Magic byte != 0, found $magicByte")
    }

    val schemaId = ByteBuffer.wrap(message, 1, 4).getInt
    if (!allowedSchemaIds.contains(schemaId)) {
      throw new RuntimeException(s"Schema ID $schemaId is not allowed!")
    }

    val avroData = message.slice(5, message.length)
    val avroSchema: Schema = schemaRegistryClient.getById(schemaId) match {
      case s: Schema => s
      case _         => throw new RuntimeException(s"Schema ID $schemaId not found")
    }

    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](avroSchema)
    val decoder = DecoderFactory.get().binaryDecoder(avroData, null)
    reader.read(null, decoder)
  }

  override def isEndOfStream(nextElement: GenericRecord): Boolean = false

  override def getProducedType: TypeInformation[GenericRecord] =
    TypeInformation.of(classOf[GenericRecord])
}

/**
  * Flink Kafka Consumer App
  */
object FlinkKafkaConsumerAppMultipleSchemas {
  val logger = LoggerFactory.getLogger(FlinkKafkaConsumerAppMultipleSchemas.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()

    val allowedSchemaIds: Set[Int] = Set(1, 2, 102, 103)
    val schemaRegistryUrl = "http://localhost:8081"
    val avroDeserializer = new AvroDeserializationSchemaMultiple(schemaRegistryUrl, allowedSchemaIds)

    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers("localhost:9092")
      .setTopics("orders")
      .setGroupId("flink-group")
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(avroDeserializer))
      .build()

    logger.info("Kafka Source configured.")

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    logger.info("Consuming data from Kafka...")

    val batchBuffer = scala.collection.mutable.ListBuffer[GenericRecord]()

    stream
      .map { record =>
        logger.info(s"Received valid Avro data: $record")
        batchBuffer += record

        if (batchBuffer.size >= 10) {
          sendToAPI(batchBuffer.toList)
          batchBuffer.clear()
        }
        record
      }
      .print()

    env.execute("Flink Kafka Consumer Multiple Schemas Validation")
  }

  def sendToAPI(records: List[GenericRecord]): Unit = {
    val httpClient: CloseableHttpClient = HttpClients.createDefault()
    val post = new HttpPost("http://localhost:8080/batch")

    val objectMapper = new ObjectMapper()
    val jsonListString = objectMapper.writeValueAsString(records.map(_.toString).asJava)

    post.setEntity(new StringEntity(jsonListString, "UTF-8"))
    post.setHeader("Content-type", "application/json")

    val response: CloseableHttpResponse = httpClient.execute(post)
    try {
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode != 200) {
        logger.error(s"Failed to send records! HTTP Code: $statusCode")
      } else {
        logger.info(s"Successfully sent ${records.size} records to API.")
      }
    } finally {
      response.close()
      httpClient.close()
    }
  }
}
