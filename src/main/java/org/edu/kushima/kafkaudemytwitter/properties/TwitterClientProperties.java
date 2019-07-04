package org.edu.kushima.kafkaudemytwitter.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data 
@Configuration
@ConfigurationProperties(prefix="twitter")
public class TwitterClientProperties {

    private String token;
    private String tokenSecret;
    private Consumer consumer;

    @Data 
    public static class Consumer {

        private String key;
        private String secret;
    }
}