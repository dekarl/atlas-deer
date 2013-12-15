package org.atlasapi.messaging;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import com.metabroadcast.common.properties.Configurer;

@Configuration
@Import({AtlasPersistenceModule.class, AtlasMessagingModule.class})
public class WorkersModule {

    private static final String INDEXER_CONSUMER = "Indexer";
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private int indexingConsumers = Integer.parseInt(Configurer.get("messaging.consumers.indexing").get());
    
//    private String loggerDestination = Configurer.get("messaging.destination.logger").get();
//    private int loggerConsumers = Integer.parseInt(Configurer.get("messaging.consumers.logger").get());
//    private long replayInterruptThreshold = Long.parseLong(Configurer.get("messaging.replay.interrupt.threshold").get());

    @Autowired private AtlasMessagingModule messaging;
    @Autowired private AtlasPersistenceModule persistence;

    @Bean
    @Lazy(true)
    public ReplayingWorker contentIndexingWorker() {
        return new ReplayingWorker(new ContentIndexingWorker(persistence.contentStore(), persistence.contentIndex()));
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerMessageListener() {
        return messaging.queueHelper().makeVirtualTopicConsumer(contentIndexingWorker(), INDEXER_CONSUMER, contentChanges, indexingConsumers, indexingConsumers);
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerReplayListener() {
        return messaging.queueHelper().makeReplayContainer(contentIndexingWorker(), "Content.Indexer", 1, 1);
    }

    @Bean
    @Lazy(true)
    public ReplayingWorker topicIndexingWorker() {
        return new ReplayingWorker(new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerMessageListener() {
        return messaging.queueHelper().makeVirtualTopicConsumer(topicIndexingWorker(), INDEXER_CONSUMER, topicChanges, indexingConsumers, indexingConsumers);
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerReplayListener() {
        return messaging.queueHelper().makeReplayContainer(topicIndexingWorker(), "Topics.Indexer", 1, 1);
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
        contentIndexingWorker().start();
    }

    @PreDestroy
    public void stop() {
        contentIndexingWorker().stop();
    }

}
