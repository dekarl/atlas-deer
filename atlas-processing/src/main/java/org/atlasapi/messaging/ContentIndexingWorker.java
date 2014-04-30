package org.atlasapi.messaging;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.util.Resolved;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class ContentIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ContentResolver contentResolver;
    private final ContentIndex contentIndex;

    public ContentIndexingWorker(ContentResolver contentResolver, ContentIndex contentIndex) {
        this.contentResolver = contentResolver;
        this.contentIndex = contentIndex;
    }

    @Override
    public void process(final ResourceUpdatedMessage message) {
        Futures.addCallback(resolveContent(message), 
            new FutureCallback<Resolved<Content>>() {
    
                @Override
                public void onFailure(Throwable throwable) {
                    log.error("iqqndexing error:", throwable);
                }
    
                @Override
                public void onSuccess(Resolved<Content> results) {
                    Optional<Content> content = results.getResources().first();
                    if (content.isPresent()) {
                        Content source = content.get();
                        log.debug("indexing {}", source);
                        try {
                            contentIndex.index(source);
                        } catch (Throwable ie) {
                            onFailure(ie);
                        }
                    }
                }
        });
    }

    private ListenableFuture<Resolved<Content>> resolveContent(final ResourceUpdatedMessage message) {
        return contentResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}