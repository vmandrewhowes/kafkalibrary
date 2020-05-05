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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
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
	public KafkaTemplate<String, T> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
//	###############
	
	
	
	
//	CONSUMER CONFIG
//	###############
	public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultSynchronousGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        return props;
    }
	
	@Bean
    public ConsumerFactory<String, Object> consumerFactory() {
    	JsonDeserializer<Object> json = new JsonDeserializer<>(Object.class);
    	
    	json.addTrustedPackages("*");
    	json.setUseTypeHeaders(false);
    	
        return new DefaultKafkaConsumerFactory<>(
                consumerConfigs(),
                new StringDeserializer(),
                json);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
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
//	@Bean
//	public ReplyingKafkaTemplate<String, T, T> replyKafkaTemplate(ProducerFactory<String, T> pf,KafkaMessageListenerContainer<String, T> container) {
//		return new ReplyingKafkaTemplate<>(producerFactory(), container);
//	}
//
//	@Bean
//	public KafkaMessageListenerContainer<String, Object> replyContainer(ConsumerFactory<String, Object> cf) {
//		ContainerProperties containerProperties = new ContainerProperties(defaultSynchronousTopic);
//		return new KafkaMessageListenerContainer<>(cf, containerProperties);
//	}
//	##################
}
