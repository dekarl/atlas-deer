package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ForwardingContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.time.Timestamp;


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
    public <C extends Content> WriteResult<C> writeContent(C content) throws WriteException {
        WriteResult<C> result = super.writeContent(content);
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }

    private <C extends Content> void writeMessage(final WriteResult<C> result) {
        ResourceUpdatedMessage message = createEntityUpdatedMessage(result);
        try {
            sender.sendMessage(message);
        } catch (Exception e) {
            log.error(message.getUpdatedResource().toString(), e);
        }
    }
    
    private <C extends Content> ResourceUpdatedMessage createEntityUpdatedMessage(WriteResult<C> result) {
        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(result.getWriteTime().getMillis()),
                result.getResource().toRef());
    }
}
