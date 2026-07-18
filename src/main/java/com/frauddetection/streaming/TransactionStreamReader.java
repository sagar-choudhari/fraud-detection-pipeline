package com.frauddetection.streaming;

import com.frauddetection.spark.SparkSessionFactory;
import com.frauddetection.utils.ConfigLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class TransactionStreamReader {

    private static final String DELTA_PATH = ConfigLoader.get("delta.transactions.path");
    private static final String CHECKPOINT_PATH = ConfigLoader.get("checkpoint.path");

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSessionFactory.create("Transaction Stream Reader");

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
                .select("data.*")
                .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"));

        transactions.printSchema();

        StreamingQuery query = transactions.writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_PATH)
                .start(DELTA_PATH);

        query.awaitTermination();

    }
}
