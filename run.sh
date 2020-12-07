docker build -t cooking-assesment .

docker run --rm -v $(pwd)/apps:/build cooking-assesment cp /cooking/target/uber-CookingAssessment-1.0-SNAPSHOT.jar /build/

wget https://storage.googleapis.com/whats_cookin/whats_cookin.zip -O data/cooking_data.zip
if [ $? -eq 0 ]; then
    echo "--> Succesfully downloaded file"
else
    echo "[ERROR] Failed to download file"
    exit 1
fi

unzip -o data/cooking_data.zip -d data
if [ $? -eq 0 ]; then
    echo "--> Succesfully unzip"
else
    echo "[ERROR] Failed to unzip"
    exit 1
fi

docker-compose up
