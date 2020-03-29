package com.kafka.config;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

public class DefaultKafkaTemplate<K, V> extends KafkaTemplate<K, V> {

	public DefaultKafkaTemplate(ProducerFactory<K, V> producerFactory) {
		super(producerFactory);
	}
}
