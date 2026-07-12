package com.frauddetection.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;
public class TransactionStreamReader {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("Transaction Stream Reader")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        StructType transactionSchema = new StructType()
                .add("txnId",     DataTypes.StringType,  false)
                .add("userId",    DataTypes.StringType,  false)
                .add("amount",    DataTypes.DoubleType,  false)
                .add("merchant",  DataTypes.StringType,  false)
                .add("timestamp", DataTypes.StringType,  false);

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "transactions")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> transactions = kafkaStream
                .selectExpr("CAST(value AS STRING) as jsonStr")
                .select(
                        from_json(col("jsonStr"), transactionSchema).alias("data")
                )
                .select("data.*");

        transactions.printSchema();

        StreamingQuery query = transactions.writeStream()
                .format("console")
                .outputMode("append")
                .option("checkpointLocation", "checkpoint/transactions")
                .option("truncate", false)
                .start();

        query.awaitTermination();

    }
}
