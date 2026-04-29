package streaming.discord

import java.io.FileReader
import java.util.Properties

import com.opencsv.{CSVParserBuilder, CSVReaderBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import streaming.discord.config.AppConfig
import streaming.discord.model.DiscordMessage

object DiscordMessageProducer {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()

    val props = new Properties()
    props.put("bootstrap.servers", config.kafka.bootstrapServers)
    props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks",              "all")

    val producer = new KafkaProducer[String, String](props)

    try {
      streamCsvToKafka(
        producer     = producer,
        topic        = config.kafka.topic,
        filePath     = config.producer.datasetPath,
        batchSize    = config.producer.batchSize,
        batchDelayMs = config.producer.batchDelayMs
      )
    } finally {
      producer.flush()
      producer.close()
    }
  }

  private def streamCsvToKafka(
      producer:     KafkaProducer[String, String],
      topic:        String,
      filePath:     String,
      batchSize:    Int,
      batchDelayMs: Long
  ): Unit = {
    val parser = new CSVParserBuilder().withSeparator(',').build()
    val reader = new CSVReaderBuilder(new FileReader(filePath))
      .withCSVParser(parser)
      .build()

    log.info(s"Streaming dataset $filePath to topic '$topic' (batch size: $batchSize)")

    try {
      reader.readNext()

      var line  = reader.readNext()
      var batch = List.empty[Array[String]]
      var sent  = 0L
      var skipped = 0L

      while (line != null) {
        if (line.length == DiscordMessage.csvFieldCount) {
          batch = batch :+ line
        } else {
          skipped += 1
        }

        if (batch.size >= batchSize) {
          sent += sendBatch(producer, topic, batch)
          batch = List.empty
          producer.flush()
          Thread.sleep(batchDelayMs)
        }

        line = reader.readNext()
      }

      if (batch.nonEmpty) {
        sent += sendBatch(producer, topic, batch)
        producer.flush()
      }

      log.info(s"Done. Sent=$sent records, skipped=$skipped malformed lines.")
    } finally {
      reader.close()
    }
  }

  private def sendBatch(
      producer: KafkaProducer[String, String],
      topic:    String,
      batch:    List[Array[String]]
  ): Int = {
    batch.foreach { cols =>
      val msg = DiscordMessage(
        Date        = cols(0),
        Channel     = cols(1),
        ServerID    = cols(2),
        ServerName  = cols(3),
        UserID      = cols(4),
        Message     = cols(5),
        Attachments = cols(6)
      )
      val payload = Json.stringify(Json.toJson(msg))
      producer.send(new ProducerRecord[String, String](topic, msg.UserID, payload))
    }
    batch.size
  }
}
