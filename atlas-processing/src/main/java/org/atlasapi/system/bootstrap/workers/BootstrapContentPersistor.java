package org.atlasapi.system.bootstrap.workers;

import java.util.List;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Version;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;

public class BootstrapContentPersistor implements ContentWriter {

    private final ContentWriter contentWriter;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final PersistingContentVisitor visitor = new PersistingContentVisitor();
    
    public BootstrapContentPersistor(ContentWriter contentWriter, ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver) {
        this.contentWriter = contentWriter;
        this.scheduleWriter = scheduleWriter;
        this.channelResolver = channelResolver;
    }

    private final class PersistingContentVisitor extends ContentVisitorAdapter<WriteResult<? extends Content>> {

        @Override
        protected WriteResult<Container> visitContainer(Container container) {
            return contentWriter.writeContent(container);
        }

        @Override
        protected WriteResult<? extends Content> visitItem(Item item) {
            try {
                boolean written = false;
                for (Version version : item.getVersions()) {
                    for (Broadcast broadcast : version.getBroadcasts()) {
                        write(new ItemAndBroadcast(item, broadcast));
                        written = true;
                    }
                }
                if (!written) {
                    contentWriter.writeContent(item);
                }
            } catch (WriteException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends Content> WriteResult<C> writeContent(C content) {
        content.setReadHash(null);// force write
        return (WriteResult<C>) content.accept(visitor);
    }
    
    private WriteResult<? extends Content> write(ItemAndBroadcast iab) throws WriteException {
        Maybe<Channel> channel = channelResolver.fromUri(iab.getBroadcast().getBroadcastOn());
        Interval interval = interval(iab.getBroadcast());
        List<ScheduleHierarchy> items = ImmutableList.of(ScheduleHierarchy.itemOnly(iab));
        return scheduleWriter.writeSchedule(items, channel.requireValue(), interval).get(0);
    }

    private Interval interval(Broadcast broadcast) {
        return new Interval(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime());
    }


}
