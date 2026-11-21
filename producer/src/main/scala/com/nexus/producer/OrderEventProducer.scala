package com.nexus.producer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.Properties
import scala.jdk.CollectionConverters._

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
      val eventFiles = resolveEventFiles(Paths.get(eventDirectory))
      if (eventFiles.isEmpty) {
        throw new IllegalArgumentException(
          s"No JSONL event files found in $eventDirectory"
        )
      }

      println(s"Kafka producer started: bootstrap=$bootstrapServers topic=$topic")
      val publish = () => eventFiles.foreach { file =>
        Files.readAllLines(file, StandardCharsets.UTF_8).asScala
          .filter(_.trim.nonEmpty)
          .foreach { payload =>
            val json = mapper.readTree(payload)
            val orderId = requiredString(json, "order_id")
            producer.send(
              new ProducerRecord[String, String](topic, orderId, payload)
            ).get()
            val eventId = Option(json.get("event_id"))
              .filterNot(_.isNull).map(_.asText()).getOrElse("<missing>")
            println(s"${Instant.now()} published event=$eventId order=$orderId")
            Thread.sleep(intervalMs)
          }
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

  private def resolveEventFiles(directory: Path): Seq[Path] =
    if (Files.isRegularFile(directory)) Seq(directory)
    else if (Files.isDirectory(directory))
      Files.list(directory).iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(".jsonl"))
        .toSeq.sortBy(_.getFileName.toString)
    else Seq.empty

  private def requiredString(json: JsonNode, field: String): String = {
    val node = json.get(field)
    if (node == null || node.isNull || node.asText().trim.isEmpty)
      throw new IllegalArgumentException(s"Missing required field: $field")
    node.asText()
  }
}
