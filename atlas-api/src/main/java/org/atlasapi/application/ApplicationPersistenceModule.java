package org.atlasapi.application;

import org.atlasapi.application.users.LegacyAdaptingUserStore;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.users.v3.MongoUserStore;
import org.atlasapi.application.v3.MongoApplicationStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;

@Configuration
@Import({AtlasPersistenceModule.class})
public class ApplicationPersistenceModule {
    
    private int cacheMinutes = Integer.parseInt(Configurer.get("application.cache.minutes").get());
    private @Autowired DatabasedMongo mongo; 
    
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Bean
    protected LegacyApplicationStore applicationStore() {
        IdGenerator idGenerator = new MongoSequentialIdGenerator(mongo, "application");
        MongoApplicationStore legacyStore = new MongoApplicationStore(mongo, idGenerator);
        LegacyAdaptingApplicationStore store = new LegacyAdaptingApplicationStore(legacyStore, mongo, idGenerator, idCodec);
        return new CacheBackedApplicationStore(store, cacheMinutes);
    }
    
    @Bean
    protected SourceRequestStore sourceRequestStore() {
        return new MongoSourceRequestStore(mongo);
    }
    
    public @Bean
    UserStore userStore() {
        MongoUserStore legacy = new MongoUserStore(mongo);
        return new LegacyAdaptingUserStore(legacy, applicationStore(), mongo);
    }
    
}
