package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String, String> producer;
    String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.producer = kafkaProducer;
        this.topic = topic;

        this.logger.info("Wikimedia handler initialized");
    }

    @Override
    public void onOpen() throws Exception {
        // no op
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info("processing message event");
        producer.send(new ProducerRecord<>(this.topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // no op
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("error processing stream", throwable);
    }
}
