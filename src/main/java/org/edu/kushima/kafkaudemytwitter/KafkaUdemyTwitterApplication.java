package org.edu.kushima.kafkaudemytwitter;

import java.util.concurrent.ExecutionException;

import org.edu.kushima.kafkaudemytwitter.producer.TwitterProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class KafkaUdemyTwitterApplication {

	@Autowired
	private TwitterProducer producer;

	private static final Logger LOG = LoggerFactory.getLogger(KafkaUdemyTwitterApplication.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		SpringApplication.run(KafkaUdemyTwitterApplication.class, args);
	}

	@EventListener(ApplicationReadyEvent.class)
    public void EventListenerExecute(){
        producer.run();  
    }
}
