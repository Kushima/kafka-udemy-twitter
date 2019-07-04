package org.edu.kushima.kafkaudemytwitter.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.edu.kushima.kafkaudemytwitter.properties.TwitterClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TwitterProducer {

    @Autowired
    private TwitterClientProperties props;
    
    private static final Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    public void run() {
        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                LOG.info(msg);
            }
        }

        LOG.info("End of application");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
       

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
            props.getConsumer().getKey(), 
            props.getConsumer().getSecret(), 
            props.getToken(), 
            props.getTokenSecret());

        ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();

            return hosebirdClient;
    }
}