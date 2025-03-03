package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemo {
    private static final Logger log = Logger.getLogger(ProducerDemo.class.getName());

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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
        producer.send(producerRecord);
        producer.flush();

        producer.close();
    }
}
