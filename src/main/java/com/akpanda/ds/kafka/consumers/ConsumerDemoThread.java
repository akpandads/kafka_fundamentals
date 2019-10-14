package com.akpanda.ds.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumer = new ConsumerThread(latch);
        Thread thread = new Thread(myConsumer);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
            logger.info("caught shitdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            })
        );
        try {
            latch.await();
        } catch (InterruptedException e) {
           logger.error("Application is interrupted");
        }
        finally {
            logger.info("application is closing");
        }
    }
}
