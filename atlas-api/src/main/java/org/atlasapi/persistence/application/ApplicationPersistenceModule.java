package org.atlasapi.persistence.application;

import org.atlasapi.application.CacheBackedApplicationStore;
import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.SourceRequestStore;
import org.atlasapi.persistence.application.MongoApplicationStore;
import org.atlasapi.persistence.application.MongoSourceRequestStore;
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
    protected LegacyApplicationStore applicationsStore() {
        IdGenerator idGenerator = new MongoSequentialIdGenerator(adminMongo, "application");
        return new CacheBackedApplicationStore(new MongoApplicationStore(idGenerator, idCodec, adminMongo), cacheMinutes);
    }
    
    @Bean
    protected SourceRequestStore sourceRequestStore() {
        return new MongoSourceRequestStore(adminMongo);
    }
    
}
