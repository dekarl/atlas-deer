package org.atlasapi;

import org.atlasapi.application.ApplicationModule;
import org.atlasapi.application.AtlasPersistenceModule;
import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.query.QueryWebModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({
    AtlasPersistenceModule.class, 
    AtlasMessagingModule.class, 
    ApplicationModule.class,
    QueryWebModule.class,
})
public class AtlasApiModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
