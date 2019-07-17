package org.edu.kushima.kafkaudemyelasticsearch;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.edu.kushima.kafkaudemyelasticsearch.consumer.ElasticSearchConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
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
	private ElasticSearchConsumer consumer;

	private static final Logger LOG = LoggerFactory.getLogger(KafkaUdemyTwitterApplication.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		SpringApplication.run(KafkaUdemyTwitterApplication.class, args);
	}

	@EventListener(ApplicationReadyEvent.class)
	public void EventListenerExecute() throws IOException {
		RestHighLevelClient client = consumer.createClient();
		
		String jsonString = "{\"foo\":\"bar\"}";
		IndexRequest request = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

		IndexResponse response = client.index(request, RequestOptions.DEFAULT);

		String id = response.getId();
		LOG.info("ID: {}", id);

		client.close();
	}
}
