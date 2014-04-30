package org.atlasapi.system.bootstrap.workers;

import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

import java.util.List;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.RuntimeWriteException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.metabroadcast.common.base.Maybe;

public class BootstrapContentPersistor implements ContentWriter {
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentStore contentStore;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final PersistingContentVisitor visitor = new PersistingContentVisitor();
    
    public BootstrapContentPersistor(ContentStore contentStore, ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver) {
        this.contentStore = contentStore;
        this.scheduleWriter = scheduleWriter;
        this.channelResolver = channelResolver;
    }

    private final class PersistingContentVisitor extends ContentVisitorAdapter<WriteResult<? extends Content,Content>> {

        @Override
        protected WriteResult<Container, Content> visitContainer(Container container) {
            return doWriteContent(container);
        }

        @Override
        protected WriteResult<? extends Content, Content> visitItem(Item item) {
            WriteResult<? extends Content, Content> result = null;
            if (hasBroadcasts(item)) {
                result = get(writeNewBroadcasts(item));
            }
            if (result == null || !result.written()) {
                log.debug("bootstrapping {}", item);
                result = doWriteContent(item);
            }
            return result;
        }

        private WriteResult<? extends Content, Content> get(
                ListenableFuture<WriteResult<? extends Content, Content>> result) {
            try {
                return Futures.get(result, WriteException.class);
            } catch (UncheckedExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), RuntimeWriteException.class);
                throw Throwables.propagate(e);
            } catch (WriteException e) {
                throw new RuntimeWriteException(e);
            }
        }

        private WriteResult<? extends Content, Content> writeNewBroadcasts(Item item, Optional<Content> current) throws WriteException {
            WriteResult<? extends Content, Content> result = null;
            for (Broadcast broadcast : Iterables.filter(item.getBroadcasts(), ACTIVELY_PUBLISHED)) {
                if (broadcast.getSourceId() != null && !hasBroadcast(current, broadcast)) {
                    ItemAndBroadcast iab = new ItemAndBroadcast(item, broadcast);
                    log.debug("bootstrapping {}", iab);
                    result = write(iab);
                }
            }
            return result;
        }
        
        private boolean hasBroadcast(Optional<Content> current, Broadcast broadcast) {
            if (!current.isPresent()) {
                return false;
            }
            Content currentContent = current.get();
            if (currentContent instanceof Item) {
                Item currentItem = (Item) currentContent;
                if (currentItem.getBroadcasts().contains(broadcast)) {
                    return true;
                }
            }
            return false;
        }

        private ListenableFuture<WriteResult<? extends Content, Content>> writeNewBroadcasts(final Item item) {
            ListenableFuture<Resolved<Content>> resolved = contentStore.resolveIds(ImmutableList.of(item.getId()));
            return Futures.transform(resolved, 
                new Function<Resolved<Content>, WriteResult<? extends Content, Content>>() {
                    @Override
                    public WriteResult<? extends Content, Content> apply(Resolved<Content> input) {
                        Optional<Content> current = input.toMap().get(item.getId());
                        try {
                            return writeNewBroadcasts(item, current);
                        } catch (WriteException e) {
                            throw new RuntimeWriteException(e);
                        }
                    }
                }
            );
        }
    }
    
    private <C extends Content> WriteResult<C, Content> doWriteContent(C content) {
        try {
            return contentStore.writeContent(content);
        } catch (WriteException e) {
            throw new RuntimeWriteException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends Content> WriteResult<C, Content> writeContent(C content) throws WriteException {
        content.setReadHash(null);// force write
        log.debug("bootstrapping {}", content);
        try {
            return (WriteResult<C, Content>) content.accept(visitor);
        } catch (RuntimeWriteException rwe) {
            throw rwe.getCause();
        }
    }
    

    @SuppressWarnings("unchecked")
    public <C extends Content> WriteResult<C, Content> fullWriteContent(C content) throws WriteException {
        content.setReadHash(null);// force write
        log.debug("bootstrapping {}", content);
        try {
            return (WriteResult<C, Content>) content.accept(new ContentVisitorAdapter<WriteResult<? extends Content,Content>>() {
                
                @Override
                protected WriteResult<? extends Content, Content> visitContainer(Container container) {
                    return doWriteContent(container);
                }
                
                @Override
                protected WriteResult<? extends Content, Content> visitItem(Item item) {
                    WriteResult<? extends Content, Content> result = null;
                    if (hasBroadcasts(item)) {
                        for (Broadcast broadcast : item.getBroadcasts()) {
                            try {
                                result = write(new ItemAndBroadcast(item, broadcast));
                            } catch (WriteException e) {
                                throw new RuntimeWriteException(e);
                            }
                        }
                    } else {
                        result = doWriteContent(item);
                    }
                    return result;
                }
                
            });
        } catch (RuntimeWriteException rwe) {
            throw rwe.getCause();
        }
    }
    
    private WriteResult<? extends Content, Content> write(ItemAndBroadcast iab) throws WriteException {
        Maybe<Channel> channel = channelResolver.fromId(iab.getBroadcast().getChannelId().longValue());
        Interval interval = interval(iab.getBroadcast());
        List<ScheduleHierarchy> items = ImmutableList.of(ScheduleHierarchy.itemOnly(iab));
        return scheduleWriter.writeSchedule(items, channel.requireValue(), interval).get(0);
    }

    private boolean hasBroadcasts(Item item) {
        return !item.getBroadcasts().isEmpty();
    }

    private Interval interval(Broadcast broadcast) {
        return new Interval(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime());
    }


}
