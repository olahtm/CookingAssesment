package com.analytics;

import com.CookingDataImporter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TopCookingRecipesTest {

    private SparkSession initTestSparkSession(){
        SparkConf conf = new SparkConf();
        conf.setAppName("CookingDataImporter");
        conf.set("spark.sql.shuffle.partitions", "64");
        conf.setMaster("local[*]"); // use all cores for now

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("WARN");

        return sparkSession;
    }

    @Test
    public void loadTopCookingRecipesForIngredients() throws IOException {

        SparkSession sparkSession = initTestSparkSession();

        Properties prop = new Properties();
        String filename = "local-test.properties";

        try (InputStream input = TopCookingRecipes.class.getClassLoader().getResourceAsStream(filename)) {
            prop.load(input);
        }

        TopCookingRecipes topCookingRecipes = new TopCookingRecipes(sparkSession,prop);

        Dataset<Row> testInputDf1 = CookingDataImporter.preProcessInput(sparkSession.read().csv("test_data/input_test1.csv"));
        Dataset<Row> testInputDf2 = CookingDataImporter.preProcessInput(sparkSession.read().csv("test_data/input_test2.csv"));
        Dataset<Row> testInputDf3 = CookingDataImporter.preProcessInput(sparkSession.read().csv("test_data/input_test3.csv"));

        List<String> testResult1 = topCookingRecipes.loadTopCookingRecipesForIngredientsAndData(Arrays.asList("sugar", "turkey"), testInputDf1);
        List<String> testResult2 = topCookingRecipes.loadTopCookingRecipesForIngredientsAndData(Arrays.asList( "sugar", "turkey"), testInputDf2);
        List<String> testResult3 = topCookingRecipes.loadTopCookingRecipesForIngredientsAndData(Arrays.asList( "sugar", "turkey"), testInputDf3);

        // for test1 there should be no match
        assert testResult1.size() == 0;
        // for test2 first id should have two matched elements
        Assert.assertEquals(testResult2,new ArrayList<>(Arrays.asList("1","2")));
        // for test3 first id should have same matched elements as second but second has less ingredients
        Assert.assertEquals(testResult3,new ArrayList<>(Arrays.asList("2","1")));

    }


}
