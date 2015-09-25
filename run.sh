spark-submit \
  --name "Spark WordCount" \
  --class com.microstrategy.wordcount.WordCount \
  --master "spark://quickstart.cloudera:7077" \
  --deploy-mode client \
  ./target/wordcount-0.0.1-SNAPSHOT.jar \
  --inputFile "hdfs://localhost/ulyss10.txt" \
  --outputFile "hdfs://localhost/wc_output"
