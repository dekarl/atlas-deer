package org.atlasapi.system.legacy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.NullContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.topic.TopicResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

@Configuration
@Import(AtlasPersistenceModule.class)
public class LegacyPersistenceModule {
    
    @Autowired AtlasPersistenceModule persistence; 

    @Bean @Qualifier("legacy")
    public ContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = persistence.databasedMongo();
        if (mongoDb == null) {
            return NullContentResolver.get();
        }
        KnownTypeContentResolver contentResolver = new MongoContentResolver(mongoDb, legacyeEquiavlenceStore());
        return new LegacyContentResolver(legacyeEquiavlenceStore(), contentResolver);
    }
    
    @Bean @Qualifier("legacy")
    public ContentListerResourceListerAdapter legacyContentLister() {
        MongoContentLister contentLister = new MongoContentLister(persistence.databasedMongo());
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
        return new MongoTopicStore(persistence.databasedMongo());
    }
    
    @Bean @Qualifier("legacy")
    public MongoLookupEntryStore legacyeEquiavlenceStore() {
        return new MongoLookupEntryStore(persistence.databasedMongo().collection("lookup"));
    }

    @Bean @Qualifier("legacy")
    public LegacyEquivalenceLister legacyEquivalenceLister() {
        return new LegacyEquivalenceLister(legacyeEquiavlenceStore());
    }
}
