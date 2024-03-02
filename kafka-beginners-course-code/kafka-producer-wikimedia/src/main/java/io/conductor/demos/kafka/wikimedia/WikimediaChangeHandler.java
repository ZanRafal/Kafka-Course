package io.conductor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //nothing to do
    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info("Received message: " + messageEvent.getData());
        //asynchronously send data
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) {
        //nothing to do
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error: ", throwable);
    }
}
