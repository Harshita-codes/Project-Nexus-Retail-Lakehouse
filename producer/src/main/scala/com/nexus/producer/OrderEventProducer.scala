package com.nexus.producer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.Properties
import scala.jdk.CollectionConverters._

private[producer] final case class OrderEvent(
    orderId: String,
    eventId: String,
    payload: String
)

object OrderEventProducer {
  private val mapper = new ObjectMapper()

  def main(args: Array[String]): Unit = {
    val bootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "order-events")
    val intervalMs =
      sys.env.getOrElse("PRODUCER_INTERVAL_MS", "500").toLong
    val eventDirectory =
      sys.env.getOrElse("EVENT_FILE_PATH", "data/events")
    val continuous = args.contains("--continuous")
    val singlePass = args.contains("--once")

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer = new KafkaProducer[String, String](properties)
    try {
      val events = readEvents(Paths.get(eventDirectory))
      if (events.isEmpty) {
        throw new IllegalArgumentException(
          s"No JSONL event files found in $eventDirectory"
        )
      }

      println(s"Kafka producer started: bootstrap=$bootstrapServers topic=$topic")
      val publish = () => events.foreach { event =>
        producer.send(
          new ProducerRecord[String, String](topic, event.orderId, event.payload)
        ).get()
        println(s"${Instant.now()} published event=${event.eventId} order=${event.orderId}")
        Thread.sleep(intervalMs)
      }

      publish()
      if (continuous && !singlePass) {
        println("Continuous mode enabled; send Ctrl+C to stop.")
        while (true) publish()
      } else {
        producer.flush()
        println("Completed one event batch pass.")
      }
    } finally {
      producer.close()
    }
  }

  private[producer] def readEvents(directory: Path): Seq[OrderEvent] =
    resolveEventFiles(directory).flatMap { file =>
      Files.readAllLines(file, StandardCharsets.UTF_8).asScala
        .filter(_.trim.nonEmpty)
        .map { payload =>
          val json = mapper.readTree(payload)
          if (json == null || !json.isObject)
            throw new IllegalArgumentException(s"Expected a JSON object in $file")
          val orderId = requiredString(json, "order_id")
          val eventId = Option(json.get("event_id"))
            .filterNot(_.isNull).map(_.asText()).getOrElse("<missing>")
          OrderEvent(orderId, eventId, payload)
        }
    }

  private def resolveEventFiles(directory: Path): Seq[Path] =
    if (Files.isRegularFile(directory)) Seq(directory)
    else if (Files.isDirectory(directory)) {
      val files = Files.list(directory)
      try files.iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(".jsonl"))
        .toVector.sortBy(_.getFileName.toString)
      finally files.close()
    } else Seq.empty

  private def requiredString(json: JsonNode, field: String): String = {
    val node = json.get(field)
    if (node == null || node.isNull || node.asText().trim.isEmpty)
      throw new IllegalArgumentException(s"Missing required field: $field")
    node.asText().trim
  }
}
