package org.atlasapi;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.schedule.ScheduleIndex;
import org.atlasapi.topic.PopularTopicIndex;

public interface IndexModule {

    ContentIndex contentIndex();

    ScheduleIndex scheduleIndex();

    PopularTopicIndex topicSearcher();
    
    ContentTitleSearcher contentSearcher();
    
}
