package com;

import com.analytics.KPICalculator;
import com.common.ConfigArguments;
import com.CookingDataImporter;
import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.model.KPIJson;
import com.model.RecipeKPI;
import com.model.TopIngredientsKPI;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CookingAnalytics {

    public static void main(String[] args) throws JSONException, IOException {
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
        conf.setAppName("CookingAnalytics");
        conf.set("spark.sql.shuffle.partitions", "64");
//        conf.setMaster("local[*]"); // use all cores for now

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        // TODO change to log4j.properties
        sparkSession.sparkContext().setLogLevel("WARN");

        Dataset<Row> cookingData = sparkSession
                .read()
                .parquet(prop.getProperty("data.parquet.root.path"))
                .cache(); // stored data in memory for multiple KPI calculation

        KPICalculator kpiCalculator = new KPICalculator(sparkSession,prop);

        RecipeKPI longestRecipeKPIWithoutBreaks = kpiCalculator.longestRecipeWithoutBreaks(cookingData);
        RecipeKPI longestRecipeKPIWithBreaks = kpiCalculator.longestRecipeWithBreaks(cookingData);
        TopIngredientsKPI topIngredientsKPI = kpiCalculator.loadMostUsedIngredients(cookingData);
        TopIngredientsKPI topDistinctIngredientsKPI = kpiCalculator.loadDistinctlyUsedIngredients(cookingData);

        List<KPIJson> result = new ArrayList<>();
        result.add(longestRecipeKPIWithoutBreaks);
        result.add(longestRecipeKPIWithBreaks);
        result.add(topIngredientsKPI);
        result.add(topDistinctIngredientsKPI);

        persistsKPIS(result,prop.getProperty("data.kpi.json.result.path"));

        cookingData.unpersist(true);
    }

    private static void persistsKPIS(List<KPIJson> result,String path) throws JSONException, IOException {
        JSONObject jsonObjectResult = convertResultsToJSON(result);

        FileWriter fileWriter = new FileWriter(path);
        fileWriter.write(jsonObjectResult.toString());
        fileWriter.flush();
        fileWriter.close();
    }

    private static JSONObject convertResultsToJSON(List<KPIJson> resultKPIs) throws JSONException {
        JSONObject result = new JSONObject();
        for (KPIJson resultKPI : resultKPIs) {
            result.put(resultKPI.getKpiName(), resultKPI.convertToJson());
        }
        System.out.println(result);
        return result;

    }



}
