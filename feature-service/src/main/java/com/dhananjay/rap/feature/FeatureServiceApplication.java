package com.dhananjay.rap.feature;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FeatureServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FeatureServiceApplication.class, args);
    }
}
