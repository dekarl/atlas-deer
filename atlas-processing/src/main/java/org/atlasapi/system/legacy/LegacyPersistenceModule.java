package org.atlasapi.system.legacy;

import java.net.UnknownHostException;
import java.util.List;

import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.NullContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.topic.TopicResolver;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

@Configuration
public class LegacyPersistenceModule {

    private String contentReadMongoHost = Configurer.get("mongo.readOnly.host").get();
    private Integer contentReadMongoPort = Configurer.get("mongo.readOnly.port").toInt();
    private String contentReadMongoName = Configurer.get("mongo.readOnly.dbName").get();

    @Bean @Qualifier("legacy")
    public ContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = readOnlyMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb, legacyeEquiavlenceStore());
        return new LegacyContentResolver(legacyeEquiavlenceStore(), contentResolver);
    }
    
    @Bean @Qualifier("legacy")
    public ContentListerResourceListerAdapter legacyContentLister() {
        MongoContentLister contentLister = new MongoContentLister(readOnlyMongo());
        return new ContentListerResourceListerAdapter(contentLister);
    }
    
    @Bean @Qualifier("legacy")
    public TopicResolver legacyTopicResolver() {
        return new LegacyTopicResolver(legacyTopicStore(), legacyTopicStore());
    }

    @Bean @Qualifier("legacy")
    public LegacyTopicLister legacyTopicLister() {
        return new LegacyTopicLister(legacyTopicStore());
    }
    
    @Bean @Qualifier("legacy")
    public MongoTopicStore legacyTopicStore() {
        return new MongoTopicStore(readOnlyMongo());
    }
    
    @Bean @Qualifier("legacy")
    public MongoLookupEntryStore legacyeEquiavlenceStore() {
        return new MongoLookupEntryStore(readOnlyMongo().collection("lookup"));
    }

    @Bean @Qualifier("legacy")
    public LegacyEquivalenceLister legacyEquivalenceLister() {
        return new LegacyEquivalenceLister(legacyeEquiavlenceStore());
    }
    
    @Bean @Qualifier("legacy")
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
    
}
