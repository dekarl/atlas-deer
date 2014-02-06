package org.atlasapi.messaging;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.system.bootstrap.workers.LegacyMessageSerializer;
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
    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    
    private Integer defaultIndexingConsumers = Configurer.get("messaging.indexing.consumers.default").toInt();
    private Integer maxIndexingConsumers = Configurer.get("messaging.indexing.consumers.max").toInt();
    
    private String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private String equivTopic = Configurer.get("equiv.update.producer.topic").get();
    private Integer equivDefltConsumers = Configurer.get("equiv.update.consumers.default").toInt();
    private Integer equivMaxConsumers = Configurer.get("equiv.update.consumers.max").toInt();
    
//    private String loggerDestination = Configurer.get("messaging.destination.logger").get();
//    private int loggerConsumers = Integer.parseInt(Configurer.get("messaging.consumers.logger").get());
//    private long replayInterruptThreshold = Long.parseLong(Configurer.get("messaging.replay.interrupt.threshold").get());

    @Autowired private AtlasMessagingModule messaging;
    @Autowired private AtlasPersistenceModule persistence;

    @Bean
    @Lazy(true)
    public ReplayingWorker<ResourceUpdatedMessage> contentIndexingWorker() {
        return new ReplayingWorker<>(new ContentIndexingWorker(persistence.contentStore(), persistence.contentIndex()));
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerMessageListener() {
        return messaging.consumerQueueFactory().makeVirtualTopicConsumer(contentIndexingWorker(), INDEXER_CONSUMER, contentChanges, defaultIndexingConsumers, maxIndexingConsumers);
    }

    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer contentIndexerReplayListener() {
        return messaging.consumerQueueFactory().makeQueueConsumer(contentIndexingWorker(), "Content.Indexer.Replay", 1, 1);
    }

    @Bean
    @Lazy(true)
    public ReplayingWorker<ResourceUpdatedMessage> topicIndexingWorker() {
        return new ReplayingWorker<>(new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerMessageListener() {
        return messaging.consumerQueueFactory().makeVirtualTopicConsumer(topicIndexingWorker(), INDEXER_CONSUMER, topicChanges, defaultIndexingConsumers, maxIndexingConsumers);
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer topicIndexerReplayListener() {
        return messaging.consumerQueueFactory().makeQueueConsumer(topicIndexingWorker(), "Topics.Indexer.Replay", 1, 1);
    }

    @Bean
    @Lazy(true)
    public ReplayingWorker<EquivalenceGraphUpdateMessage> equivalentContentStoreGraphUpdateWorker() {
        return new ReplayingWorker<>(new EquivalentContentStoreGraphUpdateWorker(persistence.getEquivalentContentStore()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer equivalentContentStoreGraphUpdateListener() {
        return messaging.consumerQueueFactory().makeVirtualTopicConsumer(equivalentContentStoreGraphUpdateWorker(), "EquivalentContentStoreGraphs", contentEquivalenceGraphChanges, defaultIndexingConsumers, maxIndexingConsumers);
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer equivalentContentStoreGraphUpdateReplayListener() {
        return messaging.consumerQueueFactory().makeQueueConsumer(equivalentContentStoreGraphUpdateWorker(), "EquivalentContent.EquivalenceGraphs.Replay", 1, 1);
    }

    @Bean
    @Lazy(true)
    public ReplayingWorker<ResourceUpdatedMessage> equivalentContentStoreContentUpdateWorker() {
        return new ReplayingWorker<>(new EquivalentContentStoreContentUpdateWorker(persistence.getEquivalentContentStore()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer equivalentContentStoreContentUpdateListener() {
        return messaging.consumerQueueFactory().makeVirtualTopicConsumer(equivalentContentStoreContentUpdateWorker(), "EquivalentContentStoreContent", contentChanges, defaultIndexingConsumers, maxIndexingConsumers);
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer equivalentContentStoreContentUpdateReplayListener() {
        return messaging.consumerQueueFactory().makeQueueConsumer(equivalentContentStoreContentUpdateWorker(), "EquivalentContent.Content.Replay", 1, 1);
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

    @Bean
    @Lazy(true)
    public ReplayingWorker<EquivalenceAssertionMessage> contentEquivalenceUpdater() {
        return new ReplayingWorker<>(new ContentEquivalenceUpdatingWorker(persistence.getContentEquivalenceGraphStore()));
    }
    
    @Bean
    @Lazy(true)
    public DefaultMessageListenerContainer equivUpdateListener() {
        return messaging.consumerQueueFactory().makeVirtualTopicConsumer(contentEquivalenceUpdater(), 
                new LegacyMessageSerializer(),
                "EquivGraphUpdate", equivSystem, equivTopic, equivDefltConsumers, equivMaxConsumers);
    }

    @PostConstruct
    public void start() {
        contentIndexingWorker().start();
        topicIndexingWorker().start();
    }

    @PreDestroy
    public void stop() {
        contentIndexingWorker().stop();
        topicIndexingWorker().start();
    }

}
