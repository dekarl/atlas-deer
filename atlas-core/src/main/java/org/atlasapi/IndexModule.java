package org.atlasapi;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.topic.PopularTopicIndex;

public interface IndexModule {

    ContentIndex contentIndex();

    PopularTopicIndex topicSearcher();
    
    ContentTitleSearcher contentTitleSearcher();
    
}
