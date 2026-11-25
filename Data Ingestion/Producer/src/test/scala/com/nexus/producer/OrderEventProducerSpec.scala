package com.nexus.producer

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class OrderEventProducerSpec extends AnyFunSuite {
  test("reads JSONL files in filename order, skipping blank lines and other files") {
    val directory = Files.createTempDirectory("producer-events")
    val first = directory.resolve("01-events.jsonl")
    val second = directory.resolve("02-events.jsonl")
    val ignored = directory.resolve("notes.txt")

    try {
      Files.write(first, Seq("""{"event_id":"E1","order_id":"O1"}""", "", "").asJava, UTF_8)
      Files.write(
        second,
        Seq(
          """{"event_id":"E2","order_id":"O2"}""",
          """{"event_id":"E3","order_id":"O3"}"""
        ).asJava,
        UTF_8
      )
      Files.write(ignored, """{"event_id":"ignored","order_id":"ignored"}""".getBytes(UTF_8))

      val events = OrderEventProducer.readEvents(directory)

      assert(events.map(_.eventId) == Seq("E1", "E2", "E3"))
      assert(events.map(_.orderId) == Seq("O1", "O2", "O3"))
      assert(events.head.payload == """{"event_id":"E1","order_id":"O1"}""")
    } finally {
      Files.deleteIfExists(first)
      Files.deleteIfExists(second)
      Files.deleteIfExists(ignored)
      Files.deleteIfExists(directory)
    }
  }

  test("rejects an event without a non-empty order_id") {
    val directory = Files.createTempDirectory("producer-invalid-event")
    val eventFile = directory.resolve("events.jsonl")

    try {
      Files.write(eventFile, """{"event_id":"E1","order_id":" "}""".getBytes(UTF_8))

      val error = intercept[IllegalArgumentException] {
        OrderEventProducer.readEvents(directory)
      }

      assert(error.getMessage.contains("order_id"))
    } finally {
      Files.deleteIfExists(eventFile)
      Files.deleteIfExists(directory)
    }
  }

  test("all checked-in sample events can be read by the producer") {
    val events = OrderEventProducer.readEvents(
      Path.of("..", "..", "Datasets", "streaming", "order-events")
    )

    assert(events.size == 30)
    assert(events.forall(event => event.orderId.nonEmpty && event.payload.nonEmpty))
  }
}
