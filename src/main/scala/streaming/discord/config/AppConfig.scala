package streaming.discord.config

import com.typesafe.config.{Config, ConfigFactory}

final case class KafkaConfig(bootstrapServers: String, topic: String)

final case class ProducerConfig(
    datasetPath:  String,
    batchSize:    Int,
    batchDelayMs: Long
)

final case class ConsumerConfig(
    wordCountsPath:     String,
    monthlyCountsPath:  String,
    userStatsPath:      String,
    checkpointBasePath: String,
    watermarkDelay:     String,
    stopwordsPath:      String
)

final case class SparkConfig(
    appName:           String,
    master:            String,
    shufflePartitions: Int
)

final case class AppConfig(
    kafka:    KafkaConfig,
    producer: ProducerConfig,
    consumer: ConsumerConfig,
    spark:    SparkConfig
)

object AppConfig {
  def load(): AppConfig = {
    val root: Config = ConfigFactory.load().getConfig("app")

    AppConfig(
      kafka = KafkaConfig(
        bootstrapServers = root.getString("kafka.bootstrap-servers"),
        topic            = root.getString("kafka.topic")
      ),
      producer = ProducerConfig(
        datasetPath  = root.getString("producer.dataset-path"),
        batchSize    = root.getInt("producer.batch-size"),
        batchDelayMs = root.getLong("producer.batch-delay-ms")
      ),
      consumer = ConsumerConfig(
        wordCountsPath     = root.getString("consumer.word-counts-path"),
        monthlyCountsPath  = root.getString("consumer.monthly-counts-path"),
        userStatsPath      = root.getString("consumer.user-stats-path"),
        checkpointBasePath = root.getString("consumer.checkpoint-base-path"),
        watermarkDelay     = root.getString("consumer.watermark-delay"),
        stopwordsPath      = root.getString("consumer.stopwords-path")
      ),
      spark = SparkConfig(
        appName           = root.getString("spark.app-name"),
        master            = root.getString("spark.master"),
        shufflePartitions = root.getInt("spark.shuffle-partitions")
      )
    )
  }
}
