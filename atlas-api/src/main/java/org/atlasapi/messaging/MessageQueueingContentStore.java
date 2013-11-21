package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ForwardingContentStore;
import org.atlasapi.entity.util.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageQueueingContentStore extends ForwardingContentStore {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentStore.class);

    private final MessageSender sender;
    private final ContentStore delegate;

    public MessageQueueingContentStore(MessageSender sender, ContentStore delegate) {
        this.sender = checkNotNull(sender);
        this.delegate = checkNotNull(delegate);
    }
    
    @Override
    protected ContentStore delegate() {
        return delegate;
    }

    @Override
    public <C extends Content> WriteResult<C> writeContent(C content) {
        WriteResult<C> result = super.writeContent(content);
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }

    private <C extends Content> void writeMessage(final WriteResult<C> result) {
        EntityUpdatedMessage message = createEntityUpdatedMessage(result);
        try {
            sender.sendMessage(message);
        } catch (Exception e) {
            log.error(message.getEntityId(), e);
        }
    }
    
    private <C extends Content> EntityUpdatedMessage createEntityUpdatedMessage(WriteResult<C> result) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                result.getWriteTime().getMillis(),
                result.getResource().getId().toString(),
                result.getClass().getSimpleName().toLowerCase(),
                result.getResource().getPublisher().key());
    }
}
