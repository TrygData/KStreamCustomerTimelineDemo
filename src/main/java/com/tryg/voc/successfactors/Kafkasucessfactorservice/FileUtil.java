package com.tryg.voc.successfactors.Kafkasucessfactorservice;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


@Component
@Slf4j
public class FileUtil {


    private String readLines(String filename) {

        try (BufferedReader fileReader = new BufferedReader(new FileReader(filename))) {

            for (String line; (line = fileReader.readLine()) != null; ) {
                return line;
            }

        } catch (Exception e) {
            log.debug(e.getLocalizedMessage());

        }

        return null;
    }
}
