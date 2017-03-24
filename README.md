# SparkCredit

SparkCredit trains a creditability prediction model with SVM and predict creditability of incoming new credit application received from Kafka stream 

# Build
To build this app:

```
   git clone https://github.com/maochf/SparkCredit.git
   mvn clean package
```    

The build will create a spark-credit-1.0-SNAPSHOT-jar-with-dependencies.jar file under the ``target`` directory.
This can be used to deploy your app to Spark.

# Run
First Spark needs to be downloaded and set the `$SPARK_HOME` to the Spark root directory
    
    > export SPARK_HOME=<spark_root_directory>

You can train the SVM model by running the command under SparkCredit project directory:

    > $SPARK_HOME/bin/spark-submit --class "ml.Credit" --master local[2] target/spark-credit-1.0-SNAPSHOT.jar

The above command will save the model under SparkCredit/model/svm_model directory.

After the model is trained, Kafka streaming requires Zookeeper and Kafka server running. The following commands are examples of starting local Zookeeper and Kafka server under Kafka root directory:

    > bin/zookeeper-server-start.sh config/zookeeper.properties
    > bin/kafka-server-start.sh config/server.properties

Then run kafka.CreditKafkaProducer from IDE to start Kafka producer to read from data/stream.csv and send new credit application to the topic "credit".

Run the following command to consume credit application from Kafka stream at topic "credit" and make creditability prediction with model/svm_model :

    > $SPARK_HOME/bin/spark-submit --class "kafka.CreditPredictConsumer" --master local[2] \
    target/spark-credit-1.0-SNAPSHOT-jar-with-dependencies.jar model/svm_model credit