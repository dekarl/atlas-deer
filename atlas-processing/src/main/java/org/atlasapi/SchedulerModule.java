package org.atlasapi;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.webapp.scheduling.ManualTaskTrigger;

@Configuration
public class SchedulerModule {

    @Bean
    public SimpleScheduler scheduler() {
        return new SimpleScheduler();
    }
    
    @Bean
    public ManualTaskTrigger taskTrigger() {
        return new ManualTaskTrigger(scheduler());
    }
    
}
