package com.kafka.config;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation can be used in conjunction with the @KafkaListener annotation
 * in order to map a LinkedHashMap to the specified JSON object.
 * 
 * @author andrewhowes
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface KafkaMessageObject {
	
	/**
	 * {@link #type()}: The JSON object used to map to the LinkedHashMap.
	 * @return The specified object type
	 */
	public Class<?> type();
	
}
