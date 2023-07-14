import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import java.sql.Timestamp 

// User-Defined Aggregation Function for computing the daily difference. Measurements are windowed in 2-day pairs
// and their difference is calculated.

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

    // Merge two partial aggregates - This is the part that actually calculates the difference. This works ONLY if the data
    // are windowed in 2-day pairs.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer2.getDouble(0) - buffer1.getDouble(0)
    }

    // Called after all the entries are exhausted.
    override def evaluate(buffer: Row): Double = {
        buffer.getDouble(0)
    }
}
  

  val spark = SparkSession
      .builder
      .appName("Etot")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()
    
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._
    
    // Register the Value Difference UDAF.
    spark.udf.register("ValDiff", new ValueDifferenceUDAF)

    // HVAC1 measurement aggregation

    val brokerUrl = "tcp://127.0.0.1:1883"

    val topic = "IoT/energy/Airconditioning/HVAC1"

    // QoS is set to be of value 1.

    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    // Payload is split on '|' character 
    val payload_hvac1 = lines.select(split(col("payload"), " [|] ").as("payload"))

    // First item is the Timestamp of the measurement, second is the value
    val raw_hvac1 = payload_hvac1.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")
    
    // Aggregation - Sum of Energy consumption of 1 day rounded in 2 digits
    val temp = raw_hvac1.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("TotalHVAC1"))

    // Filter the entry and only keep the end date as the date entry
    val aggDayHVAC1 = temp.select((col("window").getItem("end")).as("Day"),$"TotalHVAC1")


    // HVAC2 measurement aggregation - Same process as HVAC1

    val topic2 = "IoT/energy/Airconditioning/HVAC2"

    val lines2 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic2)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_hvac2 = lines2.select(split(col("payload"), " [|] ").as("payload"))
    val raw_hvac2 = payload_hvac2.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")
    
    val temp2 = raw_hvac2.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("TotalHVAC2"))

    val aggDayHVAC2 = temp2.select((col("window").getItem("end")).as("Day"),$"TotalHVAC2")

    // MiAC1 measurement aggregation

    val topic3 = "IoT/energy/Rest_appliances/MiAC1"

    val lines3 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic3)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_miac1 = lines3.select(split(col("payload"), " [|] ").as("payload"))
    val raw_miac1 = payload_miac1.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")
    
    val temp3 = raw_miac1.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("TotalMiAC1"))

    val aggDayMiAC1 = temp3.select((col("window").getItem("end")).as("Day"),$"TotalMiAC1")

    // MiAC2 measurement aggregation

    val topic4 = "IoT/energy/Rest_appliances/MiAC2"

    val lines4 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic4)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_miac2 = lines4.select(split(col("payload"), " [|] ").as("payload"))
    val raw_miac2 = payload_miac2.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")
    
    val temp4 = raw_miac2.withWatermark("DateTime", "1 second").groupBy(
        window($"DateTime", "1 day", "1 day")
    ).agg(round(sum($"NumVal"),2).as("TotalMiAC2"))

    val aggDayMiAC2 = temp4.select((col("window").getItem("end")).as("Day"),$"TotalMiAC2")

    // ETot measurement aggregation

    val topic5 = "IoT/energy/Etot"

    val lines5 = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic5)
      .option("QoS", 1)
      .option("persistence", "memory")
      .load(brokerUrl)

    val payload_etot = lines5.select(split(col("payload"), " [|] ").as("payload"))
    val raw_etot = payload_etot.withColumn("DateTime", to_timestamp($"payload".getItem(0)))
                            .withColumn("NumVal", $"payload".getItem(1).cast("double"))
                            .drop($"payload")
    
    val diff = new ValueDifferenceUDAF()
    val dfWithDailyDiff = raw_etot.groupBy(window($"DateTime", "2 days", "1 day")).agg(diff(col("NumVal")).alias("daily_diff"))
    val etotagg = dfWithDailyDiff.select(date_sub((col("window").getItem("end")),1).as("Day"), col("daily_diff"))
    val aggDiffEtot = etotagg.where(col("daily_diff")<72000)
    
    // Write-Stream Queries
    // Write the aggregated values to topics in Kafka, port 9092, in append mode

    val query1 = aggDayHVAC1.select(to_json(struct($"Day", $"TotalHVAC1")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_hvac1")
        .option("checkpointLocation", "data/checkpoint7")
        .queryName("kafka111")
        .start()

    val query2 = aggDayHVAC2.select(to_json(struct($"Day", $"TotalHVAC2")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_hvac2")
        .option("checkpointLocation", "data/checkpoint8")
        .queryName("kafka222")
        .start()

    val query3 = aggDayMiAC1.select(to_json(struct($"Day", $"TotalMiAC1")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_miac1")
        .option("checkpointLocation", "data/checkpoint9")
        .queryName("kafka333")
        .start()
    
    val query4 = aggDayMiAC2.select(to_json(struct($"Day", $"TotalMiAC2")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggday_miac2")
        .option("checkpointLocation", "data/checkpoint10")
        .queryName("kafka444")
        .start()

    val query5 = aggDiffEtot.select(to_json(struct($"Day", $"daily_diff")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "agg_etot")
        .option("checkpointLocation", "data/checkpoint11")
        .queryName("kafka555")
        .start()
        
    spark.streams.awaitAnyTermination()

