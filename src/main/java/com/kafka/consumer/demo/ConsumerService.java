/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kafka.consumer.demo;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author milos
 */
@Service
public class ConsumerService {
    
    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;
    
    private Consumer<Long, String> consumer;
    private static final Logger LOGGER = Logger.getAnonymousLogger();
    private static final String TOPIC = "test";
    
    @PostConstruct
    private void init() {
        Properties props = kafkaConsumerConfig.createConsumerConfig();
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.count()>0) {
                records.forEach((ConsumerRecord<Long, String> r) -> {
                    Long key = r.key();
                    String value = r.value();
                    int partition = r.partition();
                    LOGGER.log(Level.INFO, "{0} -> {1}, partition {2}", new Object[]{key, value, partition});
                });
            }
            consumer.commitAsync();
        }
    }
    
}
