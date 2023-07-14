import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp 

// Schema structure for input data from Kafka
val schema = StructType(Array(
      StructField("Day", TimestampType, true),
      StructField("Totalw1", DoubleType, true)
))

val schema_tot = StructType(Array(
      StructField("Day", TimestampType, true),
      StructField("daily_diff", DoubleType, true)
))

val spark = SparkSession
      .builder
      .appName("AggregationFull")
      .getOrCreate()
    
    
import spark.implicits._
spark.conf.set("spark.sql.session.timeZone", "UTC")

val agg_day = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "aggday_w1")
            .option("checkpointLocation", "checkpoint74")
            .load()


val agg_payload = agg_day.select(from_json($"value".cast("string"), schema).as("payload"))
val aggdayw1 = agg_payload.withColumn("Day",
                                     $"payload".getItem("Day"))
                        .withColumn("Totalw1", $"payload".getItem("Totalw1"))
                        .drop($"payload") 


val agg_tot = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "agg_wtot")
            .option("checkpointLocation", "checkpoint73")
            .load()


val agg_tot_payload = agg_tot.select(from_json($"value".cast("string"), schema_tot).as("payload"))
val aggtot = agg_tot_payload.withColumn("Day2",
                                     to_timestamp($"payload".getItem("Day")))
                        .withColumn("daily_diff", $"payload".getItem("daily_diff"))
                        .drop($"payload") 

val aggdayw1_wtrmrk = aggdayw1.withWatermark("Day", "5 seconds")
val aggtot_wtrmrk = aggtot.withWatermark("Day2", "5 seconds")

// Sum of W1 daily measurements and Wtot measurement that comes ones a day are joined in the same DF based on the Day they
// are measuring. 
val joined_df = aggdayw1_wtrmrk.join(aggtot_wtrmrk, expr("""Day=Day2"""))

// Water leakage is calculated as the difference of the 2 columns.
val leakdf = joined_df.withColumn("LeakValue", $"daily_diff" - $"Totalw1")
val leakfinal = leakdf.select($"Day", round($"LeakValue",2).as("LeakValue"))

// The result is written to a Kafka topic for water leakage.
val query = leakfinal.select(to_json(struct($"Day", $"LeakValue")).as("value"))
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("topic", "leak_water")
        .option("checkpointLocation", "data/checkpoint34")
        .queryName("kafka334")
        .start()

spark.streams.awaitAnyTermination()