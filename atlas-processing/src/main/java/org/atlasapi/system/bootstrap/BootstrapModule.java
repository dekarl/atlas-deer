package org.atlasapi.system.bootstrap;

import javax.jms.ConnectionFactory;

import org.atlasapi.application.AtlasPersistenceModule;
import org.atlasapi.content.Content;
import org.atlasapi.equiv.EquivalenceRecord;
import org.atlasapi.system.bootstrap.workers.BootstrapContentPersistor;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({AtlasPersistenceModule.class, BootstrapWorkersModule.class, LegacyPersistenceModule.class})
public class BootstrapModule {

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private LegacyPersistenceModule legacy;
    @Autowired private ConnectionFactory connectionFactory;
    
    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();
        
        bootstrapController.addBootstrapPair("legacy-content", new ResourceBootstrapper<Content>(legacy.legacyContentLister()), 
            new BootstrapListenerFactory<Content>() {
                @Override
                public BootstrapListener<Content> buildWithConcurrency(int concurrencyLevel) {
                    return new ContentWritingBootstrapListener(concurrencyLevel, persistor());
                }
            }
        );
        bootstrapController.addBootstrapPair("legacy-topics", new ResourceBootstrapper<Topic>(legacy.legacyTopicLister()), 
            new BootstrapListenerFactory<Topic>() {
                @Override
                public BootstrapListener<Topic> buildWithConcurrency(int concurrencyLevel) {
                    return new TopicWritingBootstrapListener(concurrencyLevel, persistence.topicStore());
                }
            }
        );
        bootstrapController.addBootstrapPair("legacy-equiv", new ResourceBootstrapper<EquivalenceRecord>(legacy.legacyEquivalenceLister()), 
            new BootstrapListenerFactory<EquivalenceRecord>() {
                @Override
                public BootstrapListener<EquivalenceRecord> buildWithConcurrency(int concurrencyLevel) {
                    return new EquivalenceBootstrapListener(concurrencyLevel, persistence.equivalenceRecordStore());
                }
            }
        );
        return bootstrapController;
    }

    private BootstrapContentPersistor persistor() {
        return new BootstrapContentPersistor(
                persistence.contentStore(),
                persistence.scheduleStore(),
                persistence.channelStore());
    }

    @Bean
    IndividualContentBootstrapController contentBootstrapController() {
        return new IndividualContentBootstrapController(legacy.legacyContentResolver(), persistor());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        return new IndividualTopicBootstrapController(legacy.legacyTopicResolver(), persistence.topicStore());
    }
    
}
