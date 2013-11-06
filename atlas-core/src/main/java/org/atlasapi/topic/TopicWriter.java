package org.atlasapi.topic;

import org.atlasapi.entity.util.WriteResult;

public interface TopicWriter {

    WriteResult<Topic> writeTopic(Topic topic);

}
