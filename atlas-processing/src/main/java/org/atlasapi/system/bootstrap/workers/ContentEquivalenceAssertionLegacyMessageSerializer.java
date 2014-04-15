package org.atlasapi.system.bootstrap.workers;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;

import com.metabroadcast.common.time.Timestamp;


public class ContentEquivalenceAssertionLegacyMessageSerializer
    extends LegacyMessageSerializer<ContentEquivalenceAssertionMessage, EquivalenceAssertionMessage> {

    
    public ContentEquivalenceAssertionLegacyMessageSerializer() {
        super(ContentEquivalenceAssertionMessage.class);
    }

    @Override
    protected EquivalenceAssertionMessage transform(ContentEquivalenceAssertionMessage leg) {
        String mid = leg.getMessageId();
        Timestamp ts = leg.getTimestamp();
        ResourceRef subj = getSubject((ContentEquivalenceAssertionMessage)leg);
        Set<ResourceRef> adjacents = toResourceRef((ContentEquivalenceAssertionMessage)leg);
        Set<Publisher> srcs = ((ContentEquivalenceAssertionMessage)leg).getSources();
        return new EquivalenceAssertionMessage(mid, ts, subj, adjacents, srcs);
    }

}
