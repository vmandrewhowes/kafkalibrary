package com.kafka.config;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.json.JSONObject;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import lombok.extern.slf4j.Slf4j;

/**
 * This @Aspect works in conjunction with the @KafkaMessageObject annotation. Its purpose is to
 * intercept the method call annotated with @KafkaMessageObject and convert the LinkedHashMap
 * to the specified JSON Object denoted by the #type element. For example, on receiving a Kafka event
 * with data about a customer, you might want to create a Customer object annotated with @JsonProperty
 * in order to deserialize the data for the created Customer object. This aspect will perform the 
 * mapping between the LinkedHashMap and the Customer object.
 * @author andrewhowes
 *
 */
@Configuration
@Aspect
@Slf4j
public class KafkaObjectDeserializer {

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Around("(execution(* *(..))) && args(object) && @annotation(KafkaMessageObject)")
	public void prepopulateHeader(ProceedingJoinPoint joinPoint, ConsumerRecord<?,?> object) {

		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule(new Jdk8Module());
			LinkedHashMap<?,?> link = (LinkedHashMap<?,?>) object.value();
			JSONObject obj = new JSONObject(link);

			MethodSignature signature = (MethodSignature) joinPoint.getSignature();
			Method method = signature.getMethod();

			KafkaMessageObject myAnnotation = method.getAnnotation(KafkaMessageObject.class);

			ConsumerRecord<?, ?> r = new ConsumerRecord<>(object.topic(), object.partition(), object.offset(), object.timestamp(),
					object.timestampType(), object.checksum(), object.serializedKeySize(), object.serializedValueSize(),
					object.key(), mapper.readValue(obj.toString(), Class.class.cast(myAnnotation.type())));

			Object[] args = joinPoint.getArgs();
			args[0] = r;
			joinPoint.proceed(args);
		} catch (JsonProcessingException e) {
			log.error(e.toString());
			try {
				joinPoint.proceed();
			} catch (Throwable e1) {
				log.error("Could not proceed to method call: " + joinPoint.getSignature());
			}
		} catch (Throwable e) {
			log.error(e.toString());
			try {
				joinPoint.proceed();
			} catch (Throwable e1) {
				log.error("Could not proceed to method call: " + joinPoint.getSignature());
			}
		}
	}
}
