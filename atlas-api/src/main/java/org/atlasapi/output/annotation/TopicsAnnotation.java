package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.media.topic.TopicResolver;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;

public class TopicsAnnotation extends OutputAnnotation<Content> {

    private static final Function<TopicRef, Id> REF_TO_ID = new Function<TopicRef, Id>() {
        @Override
        public Id apply(TopicRef input) {
            return input.getTopic();
        }
    };
    private static Function<Topic, Id> TOPIC_ID = new Function<Topic, Id>() {

        @Override
        public Id apply(Topic input) {
            return input.getId();
        }
    };

    private final TopicResolver topicResolver;
    private final EntityWriter<Topic> topicWriter;
    
    public TopicsAnnotation(TopicResolver topicResolver, EntityWriter<Topic> topicListWriter) {
        super();
        this.topicResolver = topicResolver;
        this.topicWriter = topicListWriter;
    }

    private Iterable<Topic> resolve(List<Id> topicIds) throws IOException {
        if (topicIds.isEmpty()) { // don't even ask (the resolver)
            return ImmutableList.of();
        }
        //TODO: more specific exception, probably, please?
        return Futures.get(topicResolver.resolveIds(topicIds),
                1, TimeUnit.MINUTES, IOException.class).getResources();
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        List<TopicRef> topicRefs = entity.getTopicRefs();
        Iterable<Topic> topics = resolve(Lists.transform(topicRefs, REF_TO_ID));
        final Map<Id, Topic> topicsMap = Maps.uniqueIndex(topics, TOPIC_ID);
        
        writer.writeList(new EntityListWriter<TopicRef>() {

            @Override
            public void write(TopicRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
                writer.writeObject(topicWriter, topicsMap.get(entity.getTopic()), ctxt);
                writer.writeField("supervised", entity.isSupervised());
                writer.writeField("weighting", entity.getWeighting());
                writer.writeField("relationship", entity.getRelationship());
                writer.writeField("offset", entity.getOffset());
            }

            @Override
            public String listName() {
                return "topics";
            }

            @Override
            public String fieldName(TopicRef entity) {
                return "topicref";
            }

        }, topicRefs, ctxt);
    }

}
