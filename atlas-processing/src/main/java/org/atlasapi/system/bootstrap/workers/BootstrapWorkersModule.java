package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.application.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.messaging.QueueFactory;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import com.metabroadcast.common.properties.Configurer;

@Configuration
@Import({AtlasPersistenceModule.class, AtlasMessagingModule.class, LegacyPersistenceModule.class})
public class BootstrapWorkersModule {

    private static final String CHANGES_EQUIV_PRODUCER = "Changes.Equiv";

    private String originSystem = Configurer.get("messaging.bootstrap.system").get();

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private LegacyPersistenceModule legacy;
    @Autowired private AtlasMessagingModule messaging;
    
    @Bean @Qualifier("bootstrap")
    QueueFactory bootstrapQueueFactory() {
        return new QueueFactory(messaging.activemqConnectionFactory(), originSystem);
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer contentReadWriter() {
        ContentResolver legacyResolver = legacy.legacyContentResolver();
        BootstrapContentPersistor persistor = new BootstrapContentPersistor(
            persistence.contentStore(), persistence.scheduleStore(), persistence.channelStore());
        ContentReadWriteWorker worker = new ContentReadWriteWorker(legacyResolver, persistor);
        return bootstrapQueueFactory().makeVirtualTopicConsumer(worker, "Bootstrap", messaging.contentChanges, 1, 1);
    }

    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer topicReadWriter() {
        TopicResolver legacyResolver = legacy.legacyTopicResolver();
        TopicStore writer = persistence.topicStore();
        TopicReadWriteWorker worker = new TopicReadWriteWorker(legacyResolver, writer);
        return bootstrapQueueFactory().makeVirtualTopicConsumer(worker, "Bootstrap", messaging.topicChanges, 1, 1);
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer lookupEntryReadWriter() {
        LookupEntryStore legacyResolver = legacy.legacyeEquiavlenceStore();
        EquivalenceRecordStore writer = persistence.equivalenceRecordStore();
        LookupEntryReadWriteWorker worker = new LookupEntryReadWriteWorker(legacyResolver, writer);
        return bootstrapQueueFactory().makeVirtualTopicConsumer(worker, "Bootstrap", CHANGES_EQUIV_PRODUCER, 1, 1);
    }
    
}
