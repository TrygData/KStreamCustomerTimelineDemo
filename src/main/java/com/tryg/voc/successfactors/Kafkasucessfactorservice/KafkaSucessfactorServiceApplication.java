package com.tryg.voc.successfactors.Kafkasucessfactorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;


@SpringBootApplication
public class KafkaSucessfactorServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSucessfactorServiceApplication.class, args);
	}
}
