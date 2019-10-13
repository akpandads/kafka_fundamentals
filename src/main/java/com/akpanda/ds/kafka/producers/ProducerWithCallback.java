package com.akpanda.ds.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","hello world from callback");
        // This is the overloaded method of send which takes a new CallBack anonymous class and overrides onCompletion()
        // The below lambda expression has been substituted for above inner class
        producer.send(record, (RecordMetadata recordMetadata, Exception e)-> {
            if(e == null){
                logger.info("Metadata received : \nTopic "+ recordMetadata.topic()+"\n Partition"+recordMetadata.partition()
                                +recordMetadata.offset()+"\nOffset"+"\nTimestamp "+recordMetadata.timestamp());
            }
            else{
                logger.error("Exception while sending message to kafka",e);
            }
        });

        producer.flush();
        producer.close();
    }
}
