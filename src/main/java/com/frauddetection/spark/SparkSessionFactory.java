package com.frauddetection.spark;

import com.frauddetection.utils.ConfigLoader;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    static SparkSession instance;

    public static SparkSession create(String appName){
        if (instance == null){
            instance = SparkSession.builder()
                    .appName(appName)
                    .master(ConfigLoader.get("spark.master"))
                    .config("spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.hadoop.fs.s3a.impl",
                            "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
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
