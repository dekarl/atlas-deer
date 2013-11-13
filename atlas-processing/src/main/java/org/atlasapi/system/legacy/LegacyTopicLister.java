package org.atlasapi.system.legacy;

import org.atlasapi.entity.ResourceLister;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;

import com.google.common.collect.FluentIterable;

public class LegacyTopicLister implements ResourceLister<Topic> {

    private MongoTopicStore topicStore;

    public LegacyTopicLister(MongoTopicStore topicStore) {
        this.topicStore = topicStore;
    }

    @Override
    public FluentIterable<Topic> list() {
        return topicStore.all();
    }

}
