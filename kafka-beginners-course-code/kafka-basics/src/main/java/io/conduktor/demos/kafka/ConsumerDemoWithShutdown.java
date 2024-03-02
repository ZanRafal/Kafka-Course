package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
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

        //get reference to main thread
        final Thread mainThread = Thread.currentThread();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));



        try {

            //subscribe to topic
            consumer.subscribe(List.of(topic));

            //poll for new data
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal!");
        } finally {
            consumer.close(); //this will also commit offsets
        }
    }
}
