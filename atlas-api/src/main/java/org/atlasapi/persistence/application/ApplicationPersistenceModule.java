package org.atlasapi.persistence.application;

import org.atlasapi.application.CacheBackedApplicationStore;
import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.SourceRequestStore;
import org.atlasapi.application.users.v3.MongoUserStore;
import org.atlasapi.application.users.LegacyAdaptingUserStore;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.v3.MongoApplicationStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;

@Configuration
public class ApplicationPersistenceModule {
    
    private int cacheMinutes = Integer.parseInt(Configurer.get("application.cache.minutes").get());
    private @Autowired @Qualifier(value = "adminMongo") DatabasedMongo adminMongo; 
    
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Bean
    protected LegacyApplicationStore applicationStore() {
        IdGenerator idGenerator = new MongoSequentialIdGenerator(adminMongo, "application");
        MongoApplicationStore legacyStore = new MongoApplicationStore(adminMongo, idGenerator);
        LegacyAdaptingApplicationStore store = new LegacyAdaptingApplicationStore(legacyStore, adminMongo, idGenerator, idCodec);
        return new CacheBackedApplicationStore(store, cacheMinutes);
    }
    
    @Bean
    protected SourceRequestStore sourceRequestStore() {
        return new MongoSourceRequestStore(adminMongo);
    }
    
    public @Bean
    UserStore userStore() {
        MongoUserStore legacy = new MongoUserStore(adminMongo);
        return new LegacyAdaptingUserStore(legacy, applicationStore(), adminMongo);
    }
    
}
