package org.atlasapi;

import javax.annotation.PostConstruct;

import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.messaging.WorkersModule;
import org.atlasapi.system.HealthModule;
import org.atlasapi.system.bootstrap.BootstrapModule;
import org.atlasapi.system.debug.DebugModule;
import org.atlasapi.system.health.SystemModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({
    AtlasProcessingModule.class,
    AtlasMessagingModule.class,
    WorkersModule.class,
    HealthModule.class,
    BootstrapModule.class,
    DebugModule.class,
    SystemModule.class,
})
public class AtlasProcessingModule {

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
