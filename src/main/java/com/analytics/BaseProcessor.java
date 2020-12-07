package com.analytics;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class BaseProcessor {

    protected SparkSession spark;
    protected Properties prop;

    public BaseProcessor(SparkSession spark, Properties prop) {
        this.spark = spark;
        this.prop = prop;
    }
}
