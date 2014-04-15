package org.atlasapi.system.bootstrap;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.SchedulerModule;
import org.atlasapi.content.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.bootstrap.workers.BootstrapContentPersistor;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DayRangeGenerator;
import com.metabroadcast.common.time.SystemClock;

@Configuration
@Import({AtlasPersistenceModule.class, BootstrapWorkersModule.class, LegacyPersistenceModule.class,
    SchedulerModule.class})
public class BootstrapModule {

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private LegacyPersistenceModule legacy;
    @Autowired private SchedulerModule scheduler;
    
    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();
        
        bootstrapController.addBootstrapPair("legacy-content", new ResourceBootstrapper<Content>(legacy.legacyContentLister()), 
            new BootstrapListenerFactory<Content>() {
                @Override
                public BootstrapListener<Content> buildWithConcurrency(int concurrencyLevel) {
                    return new ContentWritingBootstrapListener(concurrencyLevel, persistor());
                }
            }
        );
        bootstrapController.addBootstrapPair("legacy-topics", new ResourceBootstrapper<Topic>(legacy.legacyTopicLister()), 
            new BootstrapListenerFactory<Topic>() {
                @Override
                public BootstrapListener<Topic> buildWithConcurrency(int concurrencyLevel) {
                    return new TopicWritingBootstrapListener(concurrencyLevel, persistence.topicStore());
                }
            }
        );
        return bootstrapController;
    }

    private BootstrapContentPersistor persistor() {
        return new BootstrapContentPersistor(
                persistence.contentStore(),
                persistence.scheduleStore(),
                persistence.channelStore());
    }

    @Bean
    IndividualContentBootstrapController contentBootstrapController() {
        return new IndividualContentBootstrapController(legacy.legacyContentResolver(), persistor());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        return new IndividualTopicBootstrapController(legacy.legacyTopicResolver(), persistence.topicStore());
    }
    
    @Bean
    ExecutorServiceScheduledTask<UpdateProgress> scheduleBootstrapTask() {
        ChannelDayScheduleBootstrapTaskFactory taskFactory = scheduleBootstrapTaskFactory();
        DayRangeGenerator dayRangeGenerator = new DayRangeGenerator().withLookAhead(7).withLookBack(7);
        Set<Publisher> sources = ImmutableSet.of(Publisher.PA);
        Supplier<Iterable<ChannelDayScheduleBootstrapTask>> supplier = 
            new SourceChannelDayTaskSupplier<ChannelDayScheduleBootstrapTask>(taskFactory, persistence.channelStore(), dayRangeGenerator, sources, new SystemClock());
        ExecutorService executor = Executors.newFixedThreadPool(10, 
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("schedule-bootstrap-%d").build());
        return new ExecutorServiceScheduledTask<UpdateProgress>(executor, supplier, 10, 1, TimeUnit.MINUTES);
    }
    
    @Bean
    ChannelDayScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelDayScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(), 
                persistence.scheduleStore(), legacy.legacyContentResolver());
    }
    
    @PostConstruct
    public void schedule() {
        scheduler.scheduler().schedule(scheduleBootstrapTask(), RepetitionRules.NEVER);
    }
    
    @Bean
    public ScheduleBootstrapController scheduleBootstrapController() {
        return new ScheduleBootstrapController(scheduleBootstrapTaskFactory(), persistence.channelStore());
    }
}
