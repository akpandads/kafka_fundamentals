package com.akpanda.ds.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;

    public ConsumerThread(CountDownLatch latch){
        this.latch = latch;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"cgroup6");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("first_topic"));
    }
    @Override
    public void run() {
        try{
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(
                    record-> {
                        logger.info(record.key()+" , "+record.value());
                        logger.info("Partition" +record.partition());
                        logger.info("Offset "+record.offset());
                    }
            );
        }
        catch (WakeupException e){
            logger.error("Received Signal Shutdown");
        }
        finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown(){
        consumer.wakeup();
    }
}
