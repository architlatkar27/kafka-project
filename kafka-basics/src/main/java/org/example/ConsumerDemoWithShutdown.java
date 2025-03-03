package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerDemoWithShutdown {
    private static final Logger log = Logger.getLogger(ConsumerDemoWithShutdown.class.getName());

    public static void main(String[] args) {
        log.info("hello world");

        String groupId = "my-java-application";
        String topic  = "demo-java";
        // connect to local kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String>  consumer = new KafkaConsumer<>(props);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, lets exit by calling consumer.wakeup()");
                consumer.wakeup();
                // join the main thread to allow exec in main
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));
            log.info("starting operation");
            while(true) {
                log.info("polling for messages");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    log.info("Key: "+record.key()+ "   Value: "+record.value());
                    log.info("Partition: "+record.partition()+"   Offset: "+record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("consumer is starting to shutdown");
        } catch (Exception e) {
            log.severe("unexpected exception");
        } finally {
            consumer.close();
        }

    }
}
