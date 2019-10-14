package com.akpanda.ds.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);
        for(int i=11;i<21;i++){
            String topic = "first_topic";
            String value = "hello "+i;
            String key = "id_"+i;

            ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);
            logger.info("Key : "+key);
            // This is the overloaded method of send which takes a new CallBack anonymous class and overrides onCompletion()
            // The below lambda expression has been substituted for above inner class
            producer.send(record, (RecordMetadata recordMetadata, Exception e)-> {
                if(e == null){
                    logger.info("Metadata received : \nTopic "+ recordMetadata.topic()+"\n Partition"+recordMetadata.partition()
                            +"\nOffset"+recordMetadata.offset()+"\nTimestamp "+recordMetadata.timestamp());
                }
                else{
                    logger.error("Exception while sending message to kafka",e);
                }
            });

        }
        producer.flush();
        producer.close();
    }
}
