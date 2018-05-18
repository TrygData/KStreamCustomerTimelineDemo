package com.tryg.kafkapoc.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PocConfig {
    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }
}
