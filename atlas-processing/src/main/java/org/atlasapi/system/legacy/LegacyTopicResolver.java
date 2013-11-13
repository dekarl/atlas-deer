package org.atlasapi.system.legacy;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;

import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;


public class LegacyTopicResolver implements TopicResolver {

    public LegacyTopicResolver(TopicStore topicStore) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ListenableFuture<Resolved<Topic>> resolveIds(Iterable<Id> ids) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OptionalMap<Alias, Topic> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        // TODO Auto-generated method stub
        return null;
    }

}
