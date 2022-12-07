package com.joseatorralba.learning.kafkaspring;

import java.util.List;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class KafkaSpringApplication /* implements CommandLineRunner */ {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	private MeterRegistry meterRegistry;
	
	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringApplication.class, args); 
	}
	
	
	
	
	///////Listener	
	
	@KafkaListener(topics ="torralba-topic", groupId = "torralba-group", 
			containerFactory = "listenerContainerFactory",	// Se define containerFactory para recibir en batch 
			properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})	// Se reciben 10 mensajes o los que haya en 4seg, lo que ocurra antes
//	public void listen(String messages)			// Para recibir los mensajes 1 a 1
//	public void listen(List<String> messages)	// Para recibir los mensajes en batch
	public void listen(List<ConsumerRecord<String, String>> messages)	{	// Para recibir en batch y además recibir toda la metainformación
		
		log.info("Receiving messages {}", messages.size());
		for (ConsumerRecord<String, String> message: messages)	{
			// Comentamos para leer mejor las métricas
//			log.info("Offset {} Partition= {} Key = {}, Value = {}", message.offset(), message.partition(), message.key(), message.value());
		}
//		log.info("Process completed");
	}
	
	
	
	///// Producer 1 (descomentar -> "implements CommandLineRunner")
	//@Override
	public void run(String... args) throws Exception {
//		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("torralba-topic", "Sample mensaje");
		
		IntStream.range(1, 100).boxed()
			.forEach(i -> kafkaTemplate.send("torralba-topic", String.valueOf(i), String.format("Sample mensaje %d", i) ));
		
		// Añadir callback
//		future.addCallback(new KafkaSendCallback<String, String>() {
//
//			@Override
//			public void onSuccess(SendResult<String, String> result) {
//				log.info("Message sent ", result.getRecordMetadata().offset());
//				
//			}
//
//			@Override
//			public void onFailure(Throwable ex) {
//				log.error("Error sending message ", ex);
//			}
//
//			@Override
//			public void onFailure(KafkaProducerException ex) {
//				// TODO Auto-generated method stub
//				
//			}
//		
//		});
	}
	
	
	//// Producer 2
	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void sendKafkaMessages()	{
		for (int i=0; i<100; i++)	{
			kafkaTemplate.send("torralba-topic", String.valueOf(i), String.format("Sample mensaje %d", i));
		}
	}
	
	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetrics() {
		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {} ",count);
	}

}
