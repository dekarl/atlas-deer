package org.atlasapi.messaging;

import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicIndex;
import org.atlasapi.topic.TopicResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class TopicIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TopicResolver topicResolver;
    private final TopicIndex topicIndex;

    public TopicIndexingWorker(TopicResolver topicResolver, TopicIndex topicIndex) {
        this.topicResolver = topicResolver;
        this.topicIndex = topicIndex;
    }

    @Override
    public void process(final ResourceUpdatedMessage message) {
        Futures.addCallback(resolveContent(message), 
            new FutureCallback<Resolved<Topic>>() {
    
                @Override
                public void onFailure(Throwable throwable) {
                    log.error("Indexing error:", throwable);
                }
    
                @Override
                public void onSuccess(Resolved<Topic> results) {
                    Optional<Topic> topic = results.getResources().first();
                    if (topic.isPresent()) {
                        Topic source = topic.get();
                        log.info("Indexing {}", source);
                        try {
                            topicIndex.index(source);
                        } catch (IndexException ie) {
                            onFailure(ie);
                        }
                    } else {
                        log.warn("{}: failed to resolved {} ",
                            new Object[]{message.getMessageId(), message.getUpdatedResource()});
                    }
                }
            }
        );
    }

    private ListenableFuture<Resolved<Topic>> resolveContent(final ResourceUpdatedMessage message) {
        return topicResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}