/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.producer.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 *
 * @author milos
 */
@Service
public class ProducerService {

    @Autowired
    private KafkaProducerConfig kafkaProducerConfig;

    private Producer producer;
    private static String TOPIC = "test";
    private long sendCount = 0;
    private static final Logger LOGGER = Logger.getAnonymousLogger();

    @PostConstruct
    private void init() {
        Properties properties = kafkaProducerConfig.createProducerParameters();
        this.producer = new KafkaProducer<>(properties);
    }

    @Scheduled(fixedDelayString = "2000")
    public void sendMessage() throws InterruptedException, ExecutionException {
        try {
            double rand = Math.random() * 16;
            long key = (int) rand;
            sendCount++;
            String value = "Sent test message number " + sendCount; 
            final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record).get();
            LOGGER.log(Level.INFO, "{0} -> {1}", new Object[]{key, value});
        } finally {
            producer.flush();
        }
    }
}
