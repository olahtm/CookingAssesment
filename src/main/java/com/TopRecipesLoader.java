package com;

import com.analytics.TopCookingRecipes;
import com.beust.jcommander.JCommander;
import com.common.ConfigArguments;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TopRecipesLoader {

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
        conf.setAppName("TopRecipesLoader");
        conf.set("spark.sql.shuffle.partitions", "64");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        // TODO change to log4j.properties
        sparkSession.sparkContext().setLogLevel("WARN");

        if (arguments.getIngredients() == null)
            return;

        List<String> ingredientsToSearch = Arrays.asList(arguments.getIngredients().split(","));

        if (ingredientsToSearch.size() == 0)
            return;

        TopCookingRecipes topCookingRecipes = new TopCookingRecipes(sparkSession, prop);
        List<String> recipes = topCookingRecipes.loadTopCookingRecipesForIngredients(ingredientsToSearch);

        System.out.println(String.format("Top recipes to search %s", recipes));


    }
}
