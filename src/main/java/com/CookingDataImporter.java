package com;

import com.beust.jcommander.JCommander;
import com.common.ConfigArguments;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CookingDataImporter {
    private final static Logger log = Logger.getLogger(CookingDataImporter.class);

    public static void main(String[] args) {

        ConfigArguments arguments = new ConfigArguments();
        JCommander jCommander = new JCommander();
        jCommander.addObject(arguments);
        jCommander.parse(args);

        Properties prop = new Properties();
        String filename = arguments.getConfigFileName() + ".properties";

        try (InputStream input = CookingDataImporter.class.getClassLoader().getResourceAsStream(filename)) {
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("CookingDataImporter");
        conf.set("spark.sql.shuffle.partitions", "64");
//        conf.setMaster("local[*]"); // use all cores for now

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("WARN");

        // check if input csv file exists
        File file = new File(prop.getProperty("data.input.path"));

        if (file == null)
            throw new RuntimeException(String.format("No input file exists on %s", prop.getProperty("data.input.path")));

        // read csv file into spark dataframe
        Dataset<Row> inputData = sparkSession
                .read()
                .csv(prop.getProperty("data.input.path"));

        // preprocess input data
        Dataset<Row> preprocessedInput = preProcessInput(inputData);

        // do some validation logic
        validateData(preprocessedInput);

        // save data to parquet
        preprocessedInput
                .write()
                .mode(SaveMode.Overwrite) // overwrite any existing parquets
                .parquet(prop.getProperty("data.parquet.root.path"));

        log.info(String.format("Finished writing data to parquet into %s", prop.getProperty("data.parquet.root.path")));

    }


    public static Dataset<Row> preProcessInput(Dataset<Row> inputDf) {


        return inputDf
                .select(
                        functions.col("_c0").as("videoID"),
                        functions.col("_c1").cast(DataTypes.LongType).as("startTime"),
                        functions.col("_c2").cast(DataTypes.LongType).as("endTime"),
                        functions.col("_c3").as("action"),
                        functions.col("_c4").as("ingredient")
                );

    }

    /**
     * Validate input data, in some cases interval overlap can be an issue, log warn message for now
     * @param inputDf, already prepared dataframe
     * @return
     */
    private static void validateData(Dataset<Row> inputDf) {
        WindowSpec windowTimeOverlap = Window.partitionBy(functions.col("videoID")).orderBy(functions.col("startTime"), functions.col("endTime"));

        Dataset<Row> overlappedData = inputDf
                .withColumn("matchId", functions.when(
                        functions.col("startTime")
                                .between(
                                        functions.lag(functions.col("startTime"), 1).over(windowTimeOverlap),
                                        functions.lag(functions.col("endTime"), 1).over(windowTimeOverlap)),
                        null)
                        .otherwise(functions.monotonically_increasing_id())
                )
                .orderBy("videoID", "startTime", "endTime")
                .where("matchId is null");

        log.warn(String.format("There are %s interval overlaps in the input data, need to handle carefully when doing decisions over timeStamp data", overlappedData.count()));
    }
}
