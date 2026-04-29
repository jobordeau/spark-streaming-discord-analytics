package streaming.discord.model

import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import play.api.libs.json.{Json, OFormat}

final case class DiscordMessage(
    Date:        String,
    Channel:     String,
    ServerID:    String,
    ServerName:  String,
    UserID:      String,
    Message:     String,
    Attachments: String
)

object DiscordMessage {
  implicit val format: OFormat[DiscordMessage] = Json.format[DiscordMessage]

  val schema: StructType = new StructType()
    .add("Date",        TimestampType)
    .add("Channel",     StringType)
    .add("ServerID",    StringType)
    .add("ServerName",  StringType)
    .add("UserID",      StringType)
    .add("Message",     StringType)
    .add("Attachments", StringType)

  val csvFieldCount: Int = 7
}
