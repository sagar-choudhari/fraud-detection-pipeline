package com.frauddetection.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    static SparkSession instance;

    public static SparkSession create(String appName){
        if (instance == null){
            instance = SparkSession.builder()
                    .appName(appName)
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "4")
                    .config("spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate();
        }
        return instance;
    }

    public static void stop(){
        if (instance != null){
            instance.stop();
            instance = null;
        }
    }

}
