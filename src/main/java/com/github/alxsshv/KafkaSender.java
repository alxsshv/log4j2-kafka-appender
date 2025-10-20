package com.github.alxsshv;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.core.config.Property;

import java.util.Properties;
import java.util.UUID;

public class KafkaSender implements AutoCloseable {

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaSender(String topic, Property[] properties) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        producerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "200");
        producerProperties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");

        for (Property property : properties) {
            if (ProducerConfig.configNames().contains(property.getName())) {
                producerProperties.setProperty(property.getName(), property.getValue());
            }
        }
        this.topic = topic;
        this.producer = new KafkaProducer<>(producerProperties);
    }

    public void send(String message) {
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, UUID.randomUUID().toString(), message);
        producer.send(kafkaRecord);
    }

    @Override
    public void close() {
        producer.close();
    }
}
