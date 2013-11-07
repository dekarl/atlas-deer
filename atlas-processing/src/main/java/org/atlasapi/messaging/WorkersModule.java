package org.atlasapi.messaging;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;

import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.messaging.Worker;
import org.atlasapi.persistence.AtlasPersistenceModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

import com.metabroadcast.common.properties.Configurer;

@Configuration
@Import({AtlasPersistenceModule.class, AtlasMessagingModule.class})
public class WorkersModule {

    private String contentIndexerDestination = Configurer.get("messaging.destination.content.indexer").get();
    private String contentIndexerReplayDestination = Configurer.get("messaging.destination.content.replay.indexer").get();
    private String topicIndexerDestination = Configurer.get("messaging.destination.topics.indexer").get();
    private String topicIndexerReplayDestination = Configurer.get("messaging.destination.topics.replay.indexer").get();
    private int indexerConsumers = Integer.parseInt(Configurer.get("messaging.consumers.indexer").get());
    private String loggerDestination = Configurer.get("messaging.destination.logger").get();
    private int loggerConsumers = Integer.parseInt(Configurer.get("messaging.consumers.logger").get());
    private long replayInterruptThreshold = Long.parseLong(Configurer.get("messaging.replay.interrupt.threshold").get());

    @Autowired private ConnectionFactory connectionFactory;
    @Autowired private AtlasPersistenceModule persistence;
    
    @Bean
    @Lazy(true)
    public ReplayingWorker contentIndexerWorker() {
        return new ReplayingWorker(new ContentIndexerWorker(persistence.contentStore(), persistence.contentIndex()));
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerMessageListener() {
        return makeContainer(contentIndexerWorker(), contentIndexerDestination, indexerConsumers, indexerConsumers);
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerReplayListener() {
        return makeContainer(contentIndexerWorker(), contentIndexerReplayDestination, 1, 1);
    }

    @Bean
    @Lazy(true)
    public ReplayingWorker topicIndexerWorker() {
        return new ReplayingWorker(new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerMessageListener() {
        return makeContainer(topicIndexerWorker(), topicIndexerDestination, indexerConsumers, indexerConsumers);
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerReplayListener() {
        return makeContainer(topicIndexerWorker(), topicIndexerReplayDestination, 1, 1);
    }

//    @Bean
//    @Lazy(true)
//    public Worker messageLogger() {
//        return new MessageLogger(messageStore);
//    }
//
//    @Bean
//    @Lazy(true)
//    public DefaultMessageListenerContainer messageLoggerMessageListener() {
//        return makeContainer(messageLogger(), loggerDestination, loggerConsumers, loggerConsumers);
//    }

    @PostConstruct
    public void start() {
        contentIndexerWorker().start();
    }

    @PreDestroy
    public void stop() {
        contentIndexerWorker().stop();
    }

    private DefaultMessageListenerContainer makeContainer(Worker worker, String destination, int consumers, int maxConsumers) {
        MessageListenerAdapter adapter = new MessageListenerAdapter(worker);
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        adapter.setDefaultListenerMethod("onMessage");
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(destination);
        container.setConcurrentConsumers(consumers);
        container.setMaxConcurrentConsumers(maxConsumers);
        container.setMessageListener(adapter);

        return container;
    }
}
