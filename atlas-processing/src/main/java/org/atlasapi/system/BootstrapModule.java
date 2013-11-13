package org.atlasapi.system;

import java.net.UnknownHostException;
import java.util.List;

import javax.jms.ConnectionFactory;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.system.bootstrap.ContentReadWriter;
import org.atlasapi.system.bootstrap.TopicReadWriter;
import org.atlasapi.persistence.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.content.NullContentResolver;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.system.bootstrap.ResourceBootstrapper;
import org.atlasapi.system.legacy.ContentListerResourceListerAdapter;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.system.legacy.LegacyTopicLister;
import org.atlasapi.system.legacy.LegacyTopicResolver;
import org.atlasapi.system.legacy.LookupEntryLister;
import org.atlasapi.topic.TopicResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

@Configuration
@Import({AtlasPersistenceModule.class, AtlasMessagingModule.class})
public class BootstrapModule {

    private String contentReadMongoHost = Configurer.get("mongo.readOnly.host").get();
    private Integer contentReadMongoPort = Configurer.get("mongo.readOnly.port").toInt();
    private String contentReadMongoName = Configurer.get("mongo.readOnly.dbName").get();
    
    private String contentReadDestination = Configurer.get("messaging.destination.content.stream").get();
    private String topicReadDestination = Configurer.get("messaging.destination.topics.stream").get();
    private String equivReadDestination = Configurer.get("messaging.destination.equiv.stream").get();
    
    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private ConnectionFactory connectionFactory;
    
    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();
        
        bootstrapController.setCassandraContentStore(persistence.contentStore());
        bootstrapController.setCassandraContentBootstrapper(cassandraContentBootstrapper());
        
        bootstrapController.setCassandraTopicStore(persistence.topicStore());
        bootstrapController.setCassandraTopicBootstrapper(cassandraTopicBootstrapper());
        
        bootstrapController.setCassandraEquivalenceRecordStore(persistence.equivalenceRecordStore());
        bootstrapController.setLookupEntryStore(new MongoLookupEntryStore(readOnlyMongo().collection("lookup")));
        bootstrapController.setCassandraLookupEntryBootstrapper(cassandraEquivalenceRecordBootstrapper());
        
        return bootstrapController;
    }

    @Bean
    IndividualContentBootstrapController contentBootstrapController() {
        return new IndividualContentBootstrapController(legacyContentResolver(), persistence.contentStore());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        MongoTopicStore legacyTopicStore = legacyTopicStore();
        LegacyTopicResolver resolver = new LegacyTopicResolver(legacyTopicStore);
        return new IndividualTopicBootstrapController(resolver, persistence.topicStore());
    }

    private ResourceBootstrapper<Topic> cassandraTopicBootstrapper() {
        ResourceLister<Topic> topicLister = new LegacyTopicLister(legacyTopicStore());
        return new ResourceBootstrapper<Topic>(topicLister );
    }

    private ResourceBootstrapper<Content> cassandraContentBootstrapper() {
        MongoContentLister contentLister = new MongoContentLister(readOnlyMongo());
        ResourceLister<Content> resources = new ContentListerResourceListerAdapter(contentLister);
        return new ResourceBootstrapper<Content>(resources);
    }  
    
    private ResourceBootstrapper<LookupEntry> cassandraEquivalenceRecordBootstrapper() {
        return new ResourceBootstrapper<LookupEntry>(new LookupEntryLister(bootstrapLookupStore()));
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer contentReadWriter() {
        return makeContainer(new ContentReadWriter(legacyContentResolver(), persistence.contentStore()), contentReadDestination, 1, 1);
    }

    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer topicReadWriter() {
        org.atlasapi.topic.TopicWriter writer = persistence.topicStore();
        TopicResolver resolver = new LegacyTopicResolver(legacyTopicStore());
        return makeContainer(new TopicReadWriter(resolver, writer), topicReadDestination, 1, 1);
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer lookupEntryReadWriter() {
        return makeContainer(new LookupEntryReadWriter(bootstrapLookupStore(), persistence.equivalenceRecordStore()), equivReadDestination, 1, 1);
    }
    
    @Bean @Qualifier("readOnly")
    protected ContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = readOnlyMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb, bootstrapLookupStore());
        return new LegacyContentResolver(bootstrapLookupStore(), contentResolver);
    }
    
    @Bean @Qualifier("readOnly")
    protected MongoTopicStore legacyTopicStore() {
        return new MongoTopicStore(readOnlyMongo());
    }
    
    @Bean @Qualifier("readOnly")
    protected MongoLookupEntryStore bootstrapLookupStore() {
        return new MongoLookupEntryStore(readOnlyMongo().collection("lookup"));
    }
    
    @Bean
    public DatabasedMongo readOnlyMongo() {
        Mongo mongo = new MongoClient(mongoHosts());
        //mongo.setReadPreference(ReadPreference.secondary());
        return new DatabasedMongo(mongo, contentReadMongoName);
    }
    
    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(contentReadMongoHost), new Function<String, ServerAddress>() {
            @Override
            public ServerAddress apply(String input) {
                try {
                    return new ServerAddress(input, contentReadMongoPort);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }
    
    private DefaultMessageListenerContainer makeContainer(Object worker, String destination, int consumers, int maxConsumers) {
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
