package com.akpanda.ds.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"cgroup4");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("first_topic"));
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic",0);
        long offsetNumber = 15l;
        consumer.seek(partitionToReadFrom,offsetNumber);
        int numberOFfMessagesToRead =5;
        int numberOFfMessagesAlreadyRead =0;
        boolean keepOnReading =true;
        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOFfMessagesAlreadyRead++;
                logger.info(record.key() + " , " + record.value());
                logger.info("Partition" + record.partition());
                logger.info("Offset " + record.offset());
                if (numberOFfMessagesAlreadyRead > numberOFfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }

        }
    }
}
