package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.AbstractWorker;
import org.atlasapi.messaging.EntityUpdatedMessage;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ContentReadWriteWorker extends AbstractWorker {

    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;

    public ContentReadWriteWorker(ContentResolver contentResolver, ContentWriter contentWriter) {
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(message.getEntityId()));
        ListenableFuture<Resolved<Content>> resolved = contentResolver.resolveIds(ids);
        Futures.addCallback(resolved, new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                for (Content content : result.getResources()) {
                    content.setReadHash(null);//force write
                    contentWriter.writeContent(content);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                throw Throwables.propagate(t);
            }
        });
    }
    
}
