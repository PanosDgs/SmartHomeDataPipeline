import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp 

class ValueDifferenceUDAF extends UserDefinedAggregateFunction {
    // Input Data Type Schema
    override def inputSchema: StructType = StructType(StructField("Value", DoubleType) :: Nil)

    // Intermediate Schema
    override def bufferSchema: StructType = StructType(
        StructField("prevValue", DoubleType) :: Nil
    )

    // Returned Data Type .
    override def dataType: DataType = DoubleType

    // Self-explaining
    override def deterministic: Boolean = true

    // This function is called whenever key changes
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0.0
    }

    // Iterate over each entry of a group
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val prevValue = buffer.getDouble(0)
        val newValue = input.getDouble(0)
        buffer(0) = newValue - prevValue 

    }

    // Merge two partial aggregates
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer2.getDouble(0) - buffer1.getDouble(0)
    }

    // Called after all the entries are exhausted.
    override def evaluate(buffer: Row): Double = {
        buffer.getDouble(0)
    }
}
  
    //val valueDiff = new ValueDifferenceUDAF()
    val brokerUrl = "tcp://127.0.0.1:1884"

    val topic = "IoT/water/Wtot"

    val spark = SparkSession
      .builder
      .appName("Wtot")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()
    
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._
    spark.udf.register("ValDiff", new ValueDifferenceUDAF)
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_wtot = lines.select(split(col("payload"), " [|] ").as("payload"))

    val raw_wtot = payload_wtot.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")

    val diff = new ValueDifferenceUDAF()
    val dfWithDailyDiff = raw_wtot.groupBy(window($"DateTime", "2 days", "1 day")).agg(round(diff(col("NumVal")),2).alias("daily_diff"))
    val wtotagg = dfWithDailyDiff.select(to_timestamp(date_sub((col("window").getItem("end")),1)).as("Day"), col("daily_diff"))
    val aggDiffWtot = wtotagg.where(col("daily_diff")<150)

    val topic2 = "IoT/water/W1"
    val w1 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic2)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_w1 = w1.select(split(col("payload"), " [|] ").as("payload"))
    val raw_w11 = payload_w1.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("Val", $"payload".getItem(1))
                            .drop($"payload")
    
    val late1 = raw_w11.where(col("Val").contains("f"))
    val late2 = late1.select(col("DateTime").cast("String").as("Rejected_Event"), regexp_replace(col("Val"), "f", "").as("Value"))
    val late_rejected = late2.select(col("Rejected_Event"), col("Value").cast("double").as("NumValue"))

    val late_rejected_event = late_rejected.groupBy(
        window($"Rejected_Event", "10 minutes", "10 minutes")
    ).agg(sum($"NumValue").as("RejectedVal")).select((col("window").getItem("end").as("RejectedEvent")), $"RejectedVal")
    
    val raw_w12 = raw_w11.where(!col("Val").contains("f"))
    val raw_w1 = raw_w12.withColumn("NumVal", $"Val".cast("double")).drop("Val")
    val temp = raw_w1.withWatermark("DateTime", "3 days").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("Totalw1"))

    val aggDayw1 = temp.select((col("window").getItem("end")).as("Day"), $"Totalw1")
    val aggdayw1_final = aggDayw1.where(col("Totalw1") > 40)

    val query2 = aggdayw1_final.select(to_json(struct($"Day", $"Totalw1")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_w1")
        .option("checkpointLocation", "data/checkpoint1")
        .queryName("kafka1")
        .start()

    val query3 = aggDiffWtot.select(to_json(struct($"Day", $"daily_diff")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("topic", "agg_wtot")
        .option("checkpointLocation", "data/checkpoint2")
        .queryName("kafka2")
        .start()
    
    val query4 = late_rejected_event.select(to_json(struct($"RejectedEvent", $"RejectedVal")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("topic", "late_rejected")
        .option("checkpointLocation", "data/checkpoint3")
        .queryName("kafka3")
        .start()

    spark.streams.awaitAnyTermination()