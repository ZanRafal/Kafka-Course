package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) {
        log.info("I am Kafka producer with callbacks!");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String key = "id_" + i;
            String value = "Hello World " + i;

            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Key: " + key + " | Partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                }
            });
        }




        //flush data - synchronous
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
