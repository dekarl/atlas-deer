package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Timestamp;

public class EquivalenceAssertionMessage extends AbstractMessage {

    private final ResourceRef subject;
    private final Set<ResourceRef> assertedAdjacents;
    private final Set<Publisher> publishers;

    public EquivalenceAssertionMessage(String messageId, Timestamp timestamp, ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> publishers) {
        super(messageId, timestamp);
        this.subject = subject;
        this.assertedAdjacents = Sets.newHashSet(assertedAdjacents);
        this.publishers = Sets.newHashSet(publishers);
    }

    public ResourceRef getSubject() {
        return subject;
    }

    public Set<ResourceRef> getAssertedAdjacents() {
        return assertedAdjacents;
    }

    public Set<Publisher> getPublishers() {
        return publishers;
    }

}
