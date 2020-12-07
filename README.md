# CookingAssesment

## 1. Setup and run

```cd CookingAssesment/```

```chmod +x run.sh```

```sudo ./run.sh```

The script builts the maven project, downloads the latest csv data and deploys a single node spark standalone cluster using https://github.com/mvillarrealb/docker-spark-cluster/blob/master/docker-compose.yml. 

### Some deployment decision motivations
- For testing purposes single node spark cluster is used
- For building maven project, docker image is used launching ubuntu instance
- Using two volumes for sending data to spark-cluster, one for data and one for the jar to submit
- run.sh downloads latest cooking data and moves it to volume
- if neweast docker-compose version is installed you can follow these steps to downgrade docker-compose version according to https://stackoverflow.com/questions/57456212/error-version-in-docker-compose-yml-is-unsupported

If runned on local machine, can check spark UI on http://localhost:9090


## 2. Run applications

To run the import process execute :

```docker exec -ti cookingassessment_spark-worker_1 /spark/bin/spark-submit --class com.CookingDataImporter --driver-memory 2g --executor-memory 1g --master spark://spark-master:7077 --total-executor-cores 1 /opt/spark-apps/uber-CookingAssessment-1.0-SNAPSHOT.jar --config single-node-docker```

This will run single spark process and export the data into local file system in parquet format, data can be checked using command
```docker exec -ti cookingassessment_spark-worker_1 ls /opt/data/cooking/```

The import part is reading csv data to spark dataframe, casting long type columns for easier processing and saving it to parquet format.

To run the analytics application and calculate some key KPI values:

```docker exec -ti cookingassessment_spark-worker_1 /spark/bin/spark-submit --class com.CookingAnalytics --driver-memory 2g --executor-memory 1g --master spark://spark-master:7077 --total-executor-cores 1 /opt/spark-apps/uber-CookingAssessment-1.0-SNAPSHOT.jar --config single-node-docker```

To check the results:
```docker exec -ti cookingassessment_spark-worker_1 cat /opt/data/kpi_json.txt```

For calculation of key KPI values I used mostly Spark Dataframe API. 
For getting the longest Recipe, my solution offers two different approaches: \
	 1. Simple approach to load the difference between the last actions end and the first start \
	 2. Same as first one, with the addition to calculate breaks. In this solution I would say that duration of one recipe should be only sum of actions.
	Used WindowFunctions to efficiently handle time overlaps between actions and detect exact breaks 

For calculation of top ingredients, there was some manual work to detect some ingredients which are not actually real ingredients and save them in bad_ingredients.csv file. I tried to detect ingredients only from 'add' actions and after filtering bad ones by a simple left_anti_join.


To run batch job to find the top matching recipes for some ingredients use: 

```docker exec -ti cookingassessment_spark-worker_1 /spark/bin/spark-submit --class com.TopRecipesLoader --driver-memory 2g --executor-memory 1g --master spark://spark-master:7077 --total-executor-cores 1 /opt/spark-apps/uber-CookingAssessment-1.0-SNAPSHOT.jar --config single-node-docker --ingredients water,salt```

--ingredients values can be changed, the result will be printed on console.
Also the API is tested in TopCookingRecipesTest.java .

In the solution I preferred recipes with most matches for ingredients, in case of equality I chose that one with less ingredients.
