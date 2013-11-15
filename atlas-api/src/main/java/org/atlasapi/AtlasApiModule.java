package org.atlasapi;

import javax.annotation.PostConstruct;

import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.persistence.AtlasPersistenceModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({AtlasPersistenceModule.class, AtlasMessagingModule.class})
public class AtlasApiModule {

    @PostConstruct
    public void setup() {
        System.out.println("Ta Da!");
    }
    
    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
