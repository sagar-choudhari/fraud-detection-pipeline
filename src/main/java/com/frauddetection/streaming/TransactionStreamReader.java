package com.frauddetection.streaming;

import com.frauddetection.spark.SparkSessionFactory;
import com.frauddetection.utils.ConfigLoader;
import io.delta.tables.DeltaTable;
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
    private static final String KAFKA_BOOTSTRAP = ConfigLoader.get("kafka.bootstrap.servers");
    private static final String KAFKA_TOPIC_TRANSACTIONS = ConfigLoader.get("kafka.topic.transactions");
    private static final String STARTING_OFFSET = ConfigLoader.get("kafka.starting.offsets");

    private static final String BRONZE_PATH_RAW = ConfigLoader.get("s3.path.bronze");
    private static final String SILVER_PATH_CURATED = ConfigLoader.get("s3.path.curated");
    private static final String SILVER_PATH_ORPHANED = ConfigLoader.get("s3.path.orphaned");
    private static final String CHECKPOINT_PATH = ConfigLoader.get("s3.path.checkpoint");

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSessionFactory.create("Transaction Stream Reader");

        spark.sparkContext().setLogLevel("ERROR");

        StructType schema = new StructType()
                .add("txnId",     DataTypes.StringType,  false)
                .add("userId",    DataTypes.StringType,  false)
                .add("amount",    DataTypes.DoubleType,  false)
                .add("merchant",  DataTypes.StringType,  false)
                .add("timestamp", DataTypes.StringType,  false);

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
                .option("startingOffset", STARTING_OFFSET)
                .load();

        Dataset<Row> transactions = kafkaStream
                .selectExpr("CAST(value AS STRING) as jsonStr")
                .select(
                        from_json(col("jsonStr"), schema).alias("data")
                )
                .select("data.*")
                .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"));

        transactions.printSchema();

        StreamingQuery query = transactions.writeStream()
                .foreachBatch((batchDF, batchId) -> {

                    if(batchDF.isEmpty()){
                        System.out.println("Batch " + batchId + " — empty, skipping.");
                        return;
                    }

                    System.out.println("Batch " + batchId + " — records: " + batchDF.count());

                    batchDF.write()
                            .format("delta")
                            .mode("append")
                            .save(BRONZE_PATH_RAW);

                    System.out.println("Batch " + batchId + " — bronze written.");

                    Dataset<Row> cleanedDF = batchDF.filter(
                            col("txnId").isNotNull()
                                    .and(col("userId").isNotNull())
                                    .and(col("amount").gt(0))
                                    .and(col("merchant").isNotNull())
                                    .and(col("timestamp").isNotNull())
                    );

                    Dataset<Row> orphanedDF = batchDF.except(cleanedDF);

                    if (!orphanedDF.isEmpty()) {
                        orphanedDF.write()
                                .format("delta")
                                .mode("append")
                                .save(SILVER_PATH_ORPHANED);
                        System.out.println("Batch " + batchId + " — orphaned records: "
                                + orphanedDF.count());
                    }

                    initializeDeltaTable(batchDF.sparkSession(), SILVER_PATH_CURATED, schema);

                    DeltaTable silverTable = DeltaTable.forPath(batchDF.sparkSession(), SILVER_PATH_CURATED);

                    silverTable.alias("target")
                            .merge(cleanedDF.alias("source"),
                                    "target.txnId = source.txnId")
                            .whenMatched()
                            .updateAll()
                            .whenNotMatched()
                            .insertAll()
                            .execute();

                    System.out.println("Batch " + batchId + " — silver merged.");
                })
                .option("checkpointLocation", CHECKPOINT_PATH)
                .start();

        query.awaitTermination();

    }

    private static void initializeDeltaTable(SparkSession spark, String path, StructType schema) {
        try {
            spark.read().format("delta").load(path);
        } catch (Exception e) {
            System.out.println("Initializing Delta table at: " + path);
            spark.createDataFrame(
                            java.util.Collections.emptyList(), schema
                    )
                    .write()
                    .format("delta")
                    .save(path);
        }
    }
}
