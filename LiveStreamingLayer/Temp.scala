import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp 

    val spark = SparkSession
      .builder
      .appName("Temp")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()

    import spark.implicits._

    
    
    
    //TH1

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val brokerUrl = "tcp://127.0.0.1:1885"

    val topic = "IoT/temperature/TH1"

    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_th1 = lines.select(split(col("payload"), " [|] ").as("payload"))

    val raw_th1 = payload_th1.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")

    val temp = raw_th1.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(avg($"NumVal"),2).as("TotalTH1"))

    val aggDayTH1 = temp.select((col("window").getItem("end")).as("Day"),$"TotalTH1")


    //TH2

    val topic2 = "IoT/temperature/TH2"

    val lines2 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic2)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_th2 = lines2.select(split(col("payload"), " [|] ").as("payload"))

    val raw_th2 = payload_th2.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")

    val temp2 = raw_th2.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(avg($"NumVal"),2).as("TotalTH2"))

    val aggDayTH2 = temp2.select((col("window").getItem("end")).as("Day"),$"TotalTH2")

    //Movement MOV1

    val topic3 = "IoT/movement/Mov1"

    val lines3 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic3)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_mov = lines3.select(split(col("payload"), " [|] ").as("payload"))

    val raw_mov = payload_mov.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")

    val temp3 = raw_mov.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("TotalMov1"))

    val aggDayMov = temp3.select((col("window").getItem("end")).as("Day"),$"TotalMov1")


    //Write-Stream Queries

    val query1 = aggDayTH1.select(to_json(struct($"Day", $"TotalTH1")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_th1")
        .option("checkpointLocation", "data/checkpoint4")
        .queryName("kafka11")
        .start()

    val query2 = aggDayTH2.select(to_json(struct($"Day", $"TotalTH2")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_th2")
        .option("checkpointLocation", "data/checkpoint5")
        .queryName("kafka22")
        .start()
    
    val query3 = aggDayMov.select(to_json(struct($"Day", $"TotalMov1")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_mov")
        .option("checkpointLocation", "data/checkpoint6")
        .queryName("kafka33")
        .start()

    spark.streams.awaitAnyTermination()