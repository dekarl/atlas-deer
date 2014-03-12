package org.atlasapi;

import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.topic.TopicStore;

public interface PersistenceModule {

    ContentStore contentStore();
    
    TopicStore topicStore();

    ScheduleStore scheduleStore();
    
    EquivalenceGraphStore contentEquivalenceGraphStore();
    
    EquivalentContentStore equivalentContentStore();
    
    EquivalentScheduleStore equivalentScheduleStore();
    
}
