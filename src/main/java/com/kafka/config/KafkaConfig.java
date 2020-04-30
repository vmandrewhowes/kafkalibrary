package com.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig<T> {

	@Value("${kafka.server:localhost:9092}")
	private String bootstrapServers;

	@Value("${kafka.default.synchronous.group.id:DEFAULT_SYNCHRONOUS_ID")
	private String defaultSynchronousGroupId;
	
	@Value("${kafka.default.synchronous.topic:DEFAULT_SYNCHRONOUS_TOPIC}")
	private String defaultSynchronousTopic;

//	PRODUCER CONFIG
//	###############
	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return props;
	}
	
	@Bean
	public ProducerFactory<String, T> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public DefaultKafkaTemplate<String, T> kafkaTemplate() {
		return new DefaultKafkaTemplate<>(producerFactory());
	}
//	###############
	
	
	
	
//	CONSUMER CONFIG
//	###############
	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultSynchronousGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringJsonMessageConverter.class);
		
//		JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<Object>(new ObjectMapper());
//		jsonDeserializer.setUseTypeHeaders(false);
//		jsonDeserializer.addTrustedPackages("*");
//		
//		
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, jsonDeserializer);

		return props;
	}
	
	@Bean
	public ConsumerFactory<String, T> consumerFactory() {
		final JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>();

		jsonDeserializer.addTrustedPackages("*");
		jsonDeserializer.setUseTypeHeaders(false);

		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), jsonDeserializer);
	}
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, T>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(consumerFactory());
		factory.setReplyTemplate(kafkaTemplate());

		return factory;
	}
	
    @Bean
    public ConsumerFactory<String, byte[]> byteArrayConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), new ByteArrayDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerByteArrayContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(byteArrayConsumerFactory());
        
        return factory;
    }
    
    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerStringContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(stringConsumerFactory());

        return factory;
    }    
//	###############
    
    

//	SYNCHRONOUS CONFIG
//	##################
	@Bean
	public ReplyingKafkaTemplate<String, T, T> replyKafkaTemplate(ProducerFactory<String, T> pf,KafkaMessageListenerContainer<String, T> container) {
		return new ReplyingKafkaTemplate<>(producerFactory(), container);
	}

	@Bean
	public KafkaMessageListenerContainer<String, Object> replyContainer(ConsumerFactory<String, Object> cf) {
		ContainerProperties containerProperties = new ContainerProperties(defaultSynchronousTopic);
		return new KafkaMessageListenerContainer<>(cf, containerProperties);
	}
//	##################
}
