package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemoWithKeys {
    private static final Logger log = Logger.getLogger(ProducerDemoWithKeys.class.getName());

    public static void main(String[] args) {
        log.info("hello world");
        // connect to local kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create kafka producer record
        for(int i = 0; i < 15; i++) {
            String key = "id_"+i%3;
            String value = "hello_world_"+i;
            String topic = "demo_java";
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topic,
                    key,
                    value
            );
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("received new metadata about msg \n" +
                                "Key: " + key + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n"
                        );
                    } else {
                        log.info(e.toString());
                    }
                }
            });
        }
        producer.flush();

        producer.close();
    }
}
