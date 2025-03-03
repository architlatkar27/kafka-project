A sample project which i used to step into the world of Kafka. 

In this project I have created a basic Kafka Producer which produces notifications about wikimedia changes. An Elasticsearch consumer on the other end collects and stores them into elasticsearch sink. 

To run this project you can execute the following commands - 

1. To run Kafka Brokers locally -

```
cd conduktor-ui
docker-compose up
```

Runs a kafka broker along with conduktor UI 

2. To run wikimedia producer -

If you are using intellij then you can simply run the WikimediaChangesProducer class, else you can run it using maven - 

```
cd kafka-producer-wikimedia
mvn exec:java -Dexec.mainClass="org.example.WikimediaChangesProducer"
```

3. To run Opensearch Consumer -

First run local instance of Opensearch using docker - 

```
cd kafka-consumer-opensearch
docker-compose up
```

Then in the same dir run the consumer - 

```
mvn exec:java -Dexec.mainClass="org.example.OpenSearchConsumer"
```




   
