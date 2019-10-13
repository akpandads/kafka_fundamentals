package com.akpanda.ds.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    /*

    There are 3 steps in creating a producer
    1. Create producer properties
    2. Create producer.
    3. create producer record
    3. Send Data

    */
    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","hello world1");
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
