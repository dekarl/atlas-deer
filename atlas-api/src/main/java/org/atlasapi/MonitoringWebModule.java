package org.atlasapi;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

import org.atlasapi.system.HealthModule;
import org.atlasapi.system.JettyHealthProbe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({HealthModule.class})
public class MonitoringWebModule {
//
//    private static final Function<Class<?>, String> TO_FQN = new Function<Class<?>, String>() {
//
//        @Override
//        public String apply(Class<?> clazz) {
//            return clazz.getCanonicalName();
//        }
//    };
//
//    @Override
//    public final void setConfigLocation(String location) {
//        super.setConfigLocations( Lists.transform(ImmutableList.of(
//                JettyHealthProbe.class, 
//                HealthModule.class
//            ), TO_FQN).toArray(new String[0]));
//    }
    
    @Autowired
    private ServletContext servletContext;
    
    @PostConstruct
    public void setContext() {
        jettyHealthProbe().setServletContext(checkNotNull(servletContext));
    }
    
    @Bean 
    public JettyHealthProbe jettyHealthProbe() {
        return new JettyHealthProbe();
    }
    
}