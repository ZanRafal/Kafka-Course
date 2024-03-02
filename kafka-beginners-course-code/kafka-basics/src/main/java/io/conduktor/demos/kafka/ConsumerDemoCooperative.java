package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);
    public static void main(String[] args) {
        log.info("Hello, World");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
        consumer.subscribe(List.of(topic));

        //poll for new data
        while (true) {
            log.info("Polling for new data");

            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + " | Value: " + record.value());
                log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
            }

        }
    }
}
