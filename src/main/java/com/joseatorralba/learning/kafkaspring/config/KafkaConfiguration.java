package com.joseatorralba.learning.kafkaspring.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableScheduling
@Slf4j
public class KafkaConfiguration {

	/////// Producer
	
	private Map<String, Object> producerProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
//		props.put(ProducerConfig.RETRIES_CONFIG, 0);
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		
		return props;
	}
	
	@Bean
	public KafkaTemplate<String, String> createTemplate() {
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProperties());
		
		// Añadir listener para métricas
		pf.addListener(new MicrometerProducerListener<String, String>(meterRegistry()));
		
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		return template;
	}
		
		
	
	///////// Consumer
	
	public Map<String, Object> consumerProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "torralba-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);	
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		
		return props;
	}
	
	@Bean
	public ConsumerFactory<String, String> consumerFactory()	{
		return new DefaultKafkaConsumerFactory<>(consumerProperties());
	}
	
	@Bean("listenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory()	{
		ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		listenerContainerFactory.setConsumerFactory(consumerFactory());
		
		// Especifica que se quieren recibir varios mensajes en bloque
		// Es en el consumer donde se especifica número mensajes y tiempo
		listenerContainerFactory.setBatchListener(true);
		
		// Especifica número de hilos concurrentes
		// Al ejecutar el ejemplo veremos el proceso desordenado (hay varios hilos concurrentes)
//		listenerContainerFactory.setConcurrency(3);
		
		return listenerContainerFactory;
	}
	
	
	///////// Métricas
	
	@Bean
	public MeterRegistry meterRegistry()	{
		PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
		return meterRegistry;
	}
	
}
