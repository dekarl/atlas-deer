package org.atlasapi.system;

import java.net.UnknownHostException;
import java.util.List;

import javax.jms.ConnectionFactory;

import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.messaging.workers.ContentReadWriter;
import org.atlasapi.messaging.workers.TopicReadWriter;
import org.atlasapi.persistence.AtlasPersistenceModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.NullContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.system.bootstrap.ContentBootstrapper;
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
import com.mongodb.Mongo;
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
        bootstrapController.setLookupEntryStore(new MongoLookupEntryStore(readOnlyMongo()));
        bootstrapController.setCassandraLookupEntryBootstrapper(cassandraEquivalenceRecordBootstrapper());
        
        return bootstrapController;
    }

    @Bean
    IndividualContentBootstrapController contentBootstrapController() {
        return new IndividualContentBootstrapController(bootstrapContentResolver(), persistence.contentStore());
    }

    private ContentBootstrapper cassandraTopicBootstrapper() {
        ContentBootstrapper contentBootstrapper = new ContentBootstrapper();
        contentBootstrapper.withTopicListers(bootstrapTopicStore());
        return contentBootstrapper;
    }

    private ContentBootstrapper cassandraContentBootstrapper() {
        ContentBootstrapper contentBootstrapper = new ContentBootstrapper();
        contentBootstrapper.withContentListers(new MongoContentLister(readOnlyMongo()));
        return contentBootstrapper;
    }  
    
    private ContentBootstrapper cassandraEquivalenceRecordBootstrapper() {
        ContentBootstrapper contentBootstrapper = new ContentBootstrapper();
        contentBootstrapper.withLookupEntryListers(bootstrapLookupStore());
        return contentBootstrapper;
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer contentReadWriter() {
        return makeContainer(new ContentReadWriter(bootstrapContentResolver(), persistence.contentStore()), contentReadDestination, 1, 1);
    }

    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer topicReadWriter() {
        return makeContainer(new TopicReadWriter(bootstrapTopicStore(), persistence.topicStore()), topicReadDestination, 1, 1);
    }
    
    @Bean
    @Lazy(true)
    DefaultMessageListenerContainer lookupEntryReadWriter() {
        return makeContainer(new LookupEntryReadWriter(bootstrapLookupStore(), persistence.equivalenceRecordStore()), equivReadDestination, 1, 1);
    }
    
    @Bean @Qualifier("readOnly")
    protected ContentResolver bootstrapContentResolver() {
        DatabasedMongo mongoDb = readOnlyMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb);
        return new LookupResolvingContentResolver(contentResolver, bootstrapLookupStore());
    }
    
    @Bean @Qualifier("readOnly")
    protected TopicStore bootstrapTopicStore() {
        return new MongoTopicStore(readOnlyMongo());
    }
    
    @Bean @Qualifier("readOnly")
    protected MongoLookupEntryStore bootstrapLookupStore() {
        return new MongoLookupEntryStore(readOnlyMongo());
    }
    
    @Bean
    public DatabasedMongo readOnlyMongo() {
        Mongo mongo = new Mongo(mongoHosts());
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
