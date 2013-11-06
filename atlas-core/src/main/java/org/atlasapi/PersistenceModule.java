package org.atlasapi;

import org.atlasapi.content.ContentStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.topic.TopicStore;

public interface PersistenceModule {

    ContentStore contentStore();
    
    TopicStore topicStore();

    ScheduleStore scheduleStore();
    
}
