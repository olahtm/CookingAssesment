package com.analytics;

import com.model.IngredientWithStats;
import com.model.RecipeKPI;
import com.model.TopIngredientsKPI;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KPICalculator extends BaseProcessor {


    public KPICalculator(SparkSession spark, Properties prop) {
        super(spark, prop);
    }

    /**
     * recipe that takes most time to cook -> for this assignment will presume that all actions for one receipe are present,
     * in their video and that there are no breaks in the cooking -> will calculate the difference between the endTime of the last action and the startTime of the first action
     * So in this case, the time differences between two actions are meant to be part of cooking
     * In case of breaks, need to split the merged intervals
     */
    public RecipeKPI longestRecipeWithoutBreaks(Dataset<Row> cookingData) {
        Row longestRecipeRow = cookingData
                .groupBy("videoID")
                .agg(
                        functions.min("startTime").as("minStartTime"),
                        functions.max("endTime").as("maxEndTime"),
                        functions.collect_set("action").as("distinctActions"),    // just for visualisation
                        functions.collect_set("ingredient").as("distinctIngredients") // just for visualisation
                )
                .withColumn("duration", functions.col("maxEndTime").minus(functions.col("minStartTime")))
                .orderBy(functions.col("duration").desc())
                .first();

        return createRecipeFromLongestDurationRow(longestRecipeRow, "longestRecipeWithoutBreaks");
    }

    private RecipeKPI createRecipeFromLongestDurationRow(Row longestRecipeRow, String kpiName) {
        RecipeKPI recipeKPI = new RecipeKPI(kpiName);
        recipeKPI.setVideoID(longestRecipeRow.getString(longestRecipeRow.fieldIndex("videoID")));
        recipeKPI.setDuration(longestRecipeRow.getLong(longestRecipeRow.fieldIndex("duration")));
        recipeKPI.setMinStartTime(longestRecipeRow.getLong(longestRecipeRow.fieldIndex("minStartTime")));
        recipeKPI.setMaxEndTime(longestRecipeRow.getLong(longestRecipeRow.fieldIndex("maxEndTime")));

        return recipeKPI;
    }


    public RecipeKPI longestRecipeWithBreaks(Dataset<Row> cookingData) {
        /**
         * if There are breaks between actions, that can be not part of the recipe time
         * in these scenario we take the duration of a recipe as sum of actions( need to handle overlaps)
         */

        WindowSpec windowTimeOverlap = Window.partitionBy(functions.col("videoID")).orderBy(functions.col("startTime"), functions.col("endTime"));

        /**
         * for getting the difference between two elements will use window function, in this case add new column to df,
         * difference -> > 0 if there is no action between two rows(let's call it break), <0 if there is interval overlap between two actions
         */

        Row longestRecipeRow = cookingData
                .withColumn("diff",
                        functions.col("startTime").minus(functions.lag(functions.col("endTime"), 1).over(windowTimeOverlap)))
                .groupBy("videoID")
                .agg(
                        functions.min("startTime").as("minStartTime"),
                        functions.max("endTime").as("maxEndTime"),
                        functions.sum(functions.when(functions.col("diff").lt(0), 0).otherwise(functions.col("diff"))).as("differences")
                        // should not count negative difference, that are overlaps
                )
                .withColumn("duration", functions.col("maxEndTime").minus(functions.col("minStartTime")).minus(functions.col("differences")))
                .orderBy(functions.col("duration").desc())
                .first();

        return createRecipeFromLongestDurationRow(longestRecipeRow, "longestRecipeWithBreaks");
    }

    public TopIngredientsKPI loadMostUsedIngredients(Dataset<Row> cookingData) {
        Dataset<Row> ingredientsGroupedDf = extractIngredientsGroupedDf(cookingData);

        // no need to cache because basic aggregation is performed twice
        List<IngredientWithStats> mostUsedIngredients = ingredientsGroupedDf
                .orderBy(functions.col("totalCount").desc())
                .limit(10)
                .collectAsList()
                .stream()
                .map(row -> new IngredientWithStats(row.getString(row.fieldIndex("ingredient")),
                        row.getLong(row.fieldIndex("totalCount")), row.getLong(row.fieldIndex("distinctRecipeCount"))))
                .collect(Collectors.toList());

        return new TopIngredientsKPI("mostUsedIngredients", mostUsedIngredients);

    }

    public TopIngredientsKPI loadDistinctlyUsedIngredients(Dataset<Row> cookingData) {
        Dataset<Row> ingredientsGroupedDf = extractIngredientsGroupedDf(cookingData);

        List<IngredientWithStats> distinctlyUsedIngredients = ingredientsGroupedDf
                .orderBy(functions.col("distinctRecipeCount").desc())
                .limit(10)
                .collectAsList()
                .stream()
                .map(row -> new IngredientWithStats(row.getString(row.fieldIndex("ingredient")),
                        row.getLong(row.fieldIndex("totalCount")), row.getLong(row.fieldIndex("distinctRecipeCount"))))
                .collect(Collectors.toList());

        return new TopIngredientsKPI("topDistinctlyUsedIngredients", distinctlyUsedIngredients);

    }

    private Dataset<Row> extractIngredientsGroupedDf(Dataset<Row> cookingData) {
        Dataset<Row> badIngredientsDf = spark
                .read()
                .csv(prop.getProperty("data.bad.ingredients"))
                .select(functions.col("_c0").as("ingredient"));

        // read bad ingredients saved in csv files, they are already filtered manually to keep only really ingredients

        return cookingData
                .where("ingredient is not null") // should be not empty value for ingredients
                .where("action in ('add')")  // check only add actions
                .groupBy("ingredient")
                .agg(
                        functions.count("action").as("totalCount"),
                        functions.countDistinct("videoID").as("distinctRecipeCount")
                ).join(badIngredientsDf, JavaConverters.collectionAsScalaIterableConverter(Arrays.asList("ingredient")).asScala().toSeq(),
                        "left_anti");
    }

}
