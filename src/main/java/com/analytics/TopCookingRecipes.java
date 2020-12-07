package com.analytics;

import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopCookingRecipes extends BaseProcessor {

    public TopCookingRecipes(SparkSession spark, Properties prop) {
        super(spark, prop);
    }

    public List<String> loadTopCookingRecipesForIngredientsAndData(List<String> ingredients,Dataset<Row> cookingData){
        return cookingData
                .where("ingredient is not null")
                .withColumn("isSearched", functions.expr(String.format("case when (%s) then 1 else 0 end", createSparkInClauseFromList(ingredients))))
                .groupBy("videoID")
                .agg(
                        functions.sum("isSearched").as("isSearchedSum"),
                        functions.collect_set("ingredient").as("allIngredients"),
                        functions.count("ingredient").as("totalIngredientsCount")
                )
                .orderBy(functions.col("isSearchedSum").desc(),functions.col("totalIngredientsCount").asc()) // if there is the same amount of elements matched, most important recipes are that ones with less ingredients
                .limit(5)
                .collectAsList()
                .stream()
                .filter(row -> row.getLong(row.fieldIndex("isSearchedSum")) > 0) // should be at least one matched element
                .map(row-> row.getString(row.fieldIndex("videoID")))
                .collect(Collectors.toList());
    }

    public List<String> loadTopCookingRecipesForIngredients(List<String> ingredients) {
        Dataset<Row> cookingData = readCookingData();

        return loadTopCookingRecipesForIngredientsAndData(ingredients,cookingData);
    }

    private String createSparkInClauseFromList(List<String> elements) {
        return "ingredient in (" + Strings.join(elements
                        .stream()
                        .map(e -> "'" + e + "'")
                        .collect(Collectors.toList()),
                ",") + ")";
    }

    private Dataset<Row> readCookingData() {
        return spark
                .read()
                .parquet(prop.getProperty("data.parquet.root.path"));
    }
}
