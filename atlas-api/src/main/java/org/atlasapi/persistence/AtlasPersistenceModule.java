package org.atlasapi.persistence;

import java.net.UnknownHostException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalenceWritingContentStore;
import org.atlasapi.content.EsContentIndex;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.equiv.EquivalentsResolver;
import org.atlasapi.equiv.IdResolverBackedEquivalentResolver;
import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.messaging.MessageQueueingContentStore;
import org.atlasapi.messaging.MessageQueueingTopicStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.schedule.EsScheduleIndex;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.atlasapi.topic.TopicStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;


@Configuration
public class AtlasPersistenceModule {

    private final String mongoHost = Configurer.get("mongo.host").get();
    private final String mongoDbName = Configurer.get("mongo.dbName").get();
    
    private final String cassandraCluster = Configurer.get("cassandra.cluster").get();
    private final String cassandraKeyspace = Configurer.get("cassandra.keyspace").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String cassandraClientThreads = Configurer.get("cassandra.clientThreads").get();
 
    private final String esSeeds = Configurer.get("elasticsearch.seeds").get();
    private final String esCluster = Configurer.get("elasticsearch.cluster").get();
    private final String esRequestTimeout = Configurer.get("elasticsearch.requestTimeout").get();
    private final Parameter processingConfig = Configurer.get("processing.config");
    
    private final String adminDbHost = Configurer.get("admin.db.host").get();
    private final String adminDbPort = Configurer.get("admin.db.port").get();
    private final String adminDbName = Configurer.get("admin.db.name").get();    

    @Resource(name = "contentChanges") private JmsTemplate contentChanges;
    @Resource(name = "topicChanges") private JmsTemplate topicChanges;

    @PostConstruct
    public void init() {
        persistenceModule().startAsync().awaitRunning();
    }

    @Bean
    public CassandraPersistenceModule persistenceModule() {
        return new CassandraPersistenceModule(Splitter.on(",").split(cassandraSeeds), 
            Integer.parseInt(cassandraPort), cassandraCluster, cassandraKeyspace, 
            Integer.parseInt(cassandraClientThreads), Integer.parseInt(cassandraConnectionTimeout), 
            idGeneratorBuilder(), new ContentHasher() {
                @Override
                public String hash(Content content) {
                    return "";
                }
            });
    }
    
    @Bean
    public ContentStore contentStore() {
        ContentStore store = persistenceModule().contentStore();
        store = new EquivalenceWritingContentStore(store, equivalenceRecordStore());
        return new MessageQueueingContentStore(contentChanges, store);
    }
    
    @Bean
    public TopicStore topicStore() {
        return new MessageQueueingTopicStore(topicChanges,
            persistenceModule().topicStore());
    }
    
    @Bean
    public ScheduleStore scheduleStore() {
        return persistenceModule().scheduleStore();
    }
    
    @Bean
    public EquivalenceRecordStore equivalenceRecordStore() {
        return persistenceModule().getEquivalenceRecordStore();
    }
    
    @Bean
    public EquivalentsResolver<Content> equivalentContentResolver() {
        return new IdResolverBackedEquivalentResolver<Content>(equivalenceRecordStore(), contentStore());
    }

    @Bean
    public ElasticSearchContentIndexModule esContentIndexModule() {
        ElasticSearchContentIndexModule module = 
                new ElasticSearchContentIndexModule(esSeeds, esCluster, Long.parseLong(esRequestTimeout));
        module.init();
        return module;
    }

    @Bean @Primary
    public DatabasedMongo databasedMongo() {
        return new DatabasedMongo(mongo(), mongoDbName);
    }

    @Bean @Primary
    public Mongo mongo() {
        Mongo mongo = new Mongo(mongoHosts());
        if (processingConfig == null || !processingConfig.toBoolean()) {
            mongo.setReadPreference(ReadPreference.secondaryPreferred());
        }
        return mongo;
    }
    
    @Bean
    @Qualifier(value = "adminMongo")
    public DatabasedMongo adminMongo() {
        ServerAddress adminAddress = null;
        try {
            adminAddress = new ServerAddress(adminDbHost, Integer.parseInt(adminDbPort));
            Mongo adminMongoDb = new Mongo(adminAddress);
            adminMongoDb.setReadPreference(ReadPreference.primary());
            DatabasedMongo adminMongo = new DatabasedMongo(adminMongoDb, adminDbName);
            return adminMongo;
        } catch (UnknownHostException e) {
            Preconditions.checkNotNull(adminAddress);
            return null;
        }
    }
    
    @Bean
    public IdGeneratorBuilder idGeneratorBuilder() {
        return new IdGeneratorBuilder() {

            @Override
            public IdGenerator generator(String sequenceIdentifier) {
                return new MongoSequentialIdGenerator(databasedMongo(), sequenceIdentifier);
            }
        };
    }

    @Bean
    @Primary
    public EsScheduleIndex scheduleIndex() {
        return esContentIndexModule().scheduleIndex();
    }

    @Bean
    @Primary
    public EsContentIndex contentIndex() {
        return esContentIndexModule().contentIndex();
    }

    @Bean
    @Primary
    public EsTopicIndex topicIndex() {
        return esContentIndexModule().topicIndex();
    }

    @Bean
    @Primary
    public EsPopularTopicIndex popularTopicIndex() {
        return esContentIndexModule().topicSearcher();
    }
    
    @Bean
    @Primary
    public EsContentTitleSearcher contentSearcher() {
        return esContentIndexModule().contentTitleSearcher();
    }

    @Bean
    @Primary
    public ChannelStore channelStore() {
        MongoChannelStore rawStore = new MongoChannelStore(databasedMongo(), channelGroupStore(), channelGroupStore());
        return new CachingChannelStore(rawStore);
    }
    
    @Bean
    @Primary
    public ChannelGroupStore channelGroupStore() {
        return new MongoChannelGroupStore(databasedMongo());
    }

    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(mongoHost), new Function<String, ServerAddress>() {

            @Override
            public ServerAddress apply(String input) {
                try {
                    return new ServerAddress(input, 27017);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }
    
}
