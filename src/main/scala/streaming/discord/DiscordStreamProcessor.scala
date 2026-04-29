package streaming.discord

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory
import streaming.discord.config.{AppConfig, SparkConfig}
import streaming.discord.model.DiscordMessage

import scala.io.Source

object DiscordStreamProcessor {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()

    implicit val spark: SparkSession = buildSparkSession(config.spark)
    import spark.implicits._

    val rawStream: Dataset[String] = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe",                config.kafka.topic)
      .option("startingOffsets",          "earliest")
      .option("failOnDataLoss",           "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val messages: DataFrame = rawStream
      .select(from_json(col("value"), DiscordMessage.schema).as("data"))
      .select("data.*")
      .withWatermark("Date", config.consumer.watermarkDelay)

    val stopWords: Set[String] = loadStopWords(config.consumer.stopwordsPath)

    val wordCounts    = computeWordCounts(messages, stopWords)
    val monthlyCounts = computeMonthlyCounts(messages)
    val userStats     = computeUserStats(messages)

    val q1 = startUpsertQuery(
      df         = wordCounts,
      tablePath  = config.consumer.wordCountsPath,
      checkpoint = s"${config.consumer.checkpointBasePath}/word_counts",
      mergeKey   = "word"
    )

    val q2 = startUpsertQuery(
      df         = monthlyCounts,
      tablePath  = config.consumer.monthlyCountsPath,
      checkpoint = s"${config.consumer.checkpointBasePath}/monthly_counts",
      mergeKey   = "Month"
    )

    val q3 = startUpsertQuery(
      df         = userStats,
      tablePath  = config.consumer.userStatsPath,
      checkpoint = s"${config.consumer.checkpointBasePath}/user_stats",
      mergeKey   = "UserID"
    )

    log.info(s"Streaming queries started: ${Seq(q1, q2, q3).map(_.name).mkString(", ")}")
    log.info(s"Reading from Kafka topic '${config.kafka.topic}' at ${config.kafka.bootstrapServers}")

    spark.streams.awaitAnyTermination()
  }

  private def buildSparkSession(cfg: SparkConfig): SparkSession =
    SparkSession.builder
      .appName(cfg.appName)
      .master(cfg.master)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions",             cfg.shufflePartitions)
      .config("spark.sql.extensions",                     "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

  private def loadStopWords(path: String): Set[String] = {
    val source = Source.fromFile(path, "UTF-8")
    try {
      source.getLines().map(_.trim.toLowerCase).filter(_.nonEmpty).toSet
    } finally {
      source.close()
    }
  }

  private def computeWordCounts(messages: DataFrame, stopWords: Set[String])
                               (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    messages
      .select(
        $"Date",
        explode(split(regexp_replace(col("Message"), "'", " "), "\\s+")).as("word")
      )
      .withColumn("word", regexp_replace($"word", "[^\\w]+", ""))
      .withColumn("word", lower($"word"))
      .filter($"word" =!= "" && !$"word".isin(stopWords.toSeq: _*))
      .groupBy("word")
      .count()
  }

  private def computeMonthlyCounts(messages: DataFrame): DataFrame =
    messages
      .withColumn("Month", date_format(col("Date"), "yyyy-MM"))
      .groupBy("Month")
      .count()

  private def computeUserStats(messages: DataFrame): DataFrame =
    messages
      .groupBy("UserID")
      .count()

  private def startUpsertQuery(
      df:         DataFrame,
      tablePath:  String,
      checkpoint: String,
      mergeKey:   String
  )(implicit spark: SparkSession): StreamingQuery =
    df.writeStream
      .outputMode("update")
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        if (batchDF.isEmpty) {
          ()
        } else if (!DeltaTable.isDeltaTable(spark, tablePath)) {
          batchDF.write.format("delta").mode("overwrite").save(tablePath)
        } else {
          DeltaTable.forPath(spark, tablePath).as("target")
            .merge(batchDF.as("source"), s"target.$mergeKey = source.$mergeKey")
            .whenMatched.updateAll()
            .whenNotMatched.insertAll()
            .execute()
        }
      }
      .start()
}
