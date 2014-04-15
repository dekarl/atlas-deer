package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.atlasapi.content.Item.ContainerSummary;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.RuntimeWriteException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

public abstract class AbstractContentStore implements ContentStore {

    private static final Content NO_PREVIOUS = null;
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final class ContentWritingVisitor implements ContentVisitor<WriteResult<? extends Content>> {

        private boolean hashChanged(Content writing, Content previous) {
            return !hasher.hash(writing).equals(hasher.hash(previous));
        }
        
        private void updateTimes(Content content) {
            DateTime now = clock.now();
            if (content.getFirstSeen() == null) {
                content.setFirstSeen(now);
            }
            content.setLastUpdated(now);
            content.setThisOrChildLastUpdated(now);
        }

        private void updateWithPevious(Content writing, Content previous) {
            writing.setId(previous.getId());
            writing.setFirstSeen(previous.getFirstSeen());
            updateTimes(writing);
        }
        
        @Override
        public WriteResult<Brand> visit(Brand brand) {
            Brand previous = (Brand) getPreviousContent(brand);

            brand.setItemRefs(ImmutableSet.<ItemRef>of());
            brand.setSeriesRefs(ImmutableSet.<SeriesRef>of());
            
            if (previous != null) {
                return writeBrandWithPrevious(brand, previous);
            }

            updateTimes(brand);
            write(brand, NO_PREVIOUS);
            
            return WriteResult.written(brand).build();
            
        }

        private WriteResult<Brand> writeBrandWithPrevious(Brand brand, Brand previous) {
            boolean written = false;
            if (hashChanged(brand, previous)) {
                updateWithPevious(brand, previous);
                write(brand, previous);
                written = true;
            } 
            brand.setItemRefs(previous.getItemRefs());
            brand.setSeriesRefs(previous.getSeriesRefs());
            return WriteResult.result(brand, written)
                .withPrevious(previous)
                .build();
        }

        @Override
        public WriteResult<Series> visit(Series series) {
            Series previous = (Series) getPreviousContent(series);
            
            series.setItemRefs(ImmutableSet.<ItemRef>of());
            if (previous != null) {
                return writeSeriesWithPrevious(series, previous);
            }
            updateTimes(series);
            writeRefAndSummarizePrimary(series);
            write(series, NO_PREVIOUS);
            return WriteResult.written(series).build();
        }

        private WriteResult<Series> writeSeriesWithPrevious(Series series, Series previous) {
            boolean written = false;
            if (hashChanged(series, previous)) {
                updateWithPevious(series, previous);
                writeRefAndSummarizePrimary(series);
                write(series, previous);
                written = true;
            }
            series.setItemRefs(previous.getItemRefs());
            return WriteResult.result(series, written)
                .withPrevious(previous)
                .build();
        }
        
        private void writeRefAndSummarizePrimary(Series series) {
            if (series.getBrandRef() != null) {
                BrandRef primary = series.getBrandRef();
                //TODO set summary on series
                ContainerSummary summarize = getSummary(primary);
                ensureId(series);
                writeSecondaryContainerRef(primary, series.toRef());
            }
        }
        
        @Override
        public WriteResult<Item> visit(Item item) {
            Item previous = (Item) getPreviousContent(item);
            
            if (previous != null) {
                return writeItemWithPrevious(item, previous);
            }
            updateTimes(item);
            writeRefAndSummarizeContainer(item);
            write(item, NO_PREVIOUS);
            return WriteResult.written(item)
                .build();
        }

        private WriteResult<Item> writeItemWithPrevious(Item item, Item previous) {
            boolean written = false;
            if (hashChanged(item, previous)) {
                updateWithPevious(item, previous);
                writeRefAndSummarizeContainer(item);
                write(item, previous);
                written = true;
            } 
            return WriteResult.result(item, written)
                .withPrevious(previous)
                .build();
        }

        private void writeRefAndSummarizeContainer(Item item) {
            if (item.getContainerRef() != null) {
                ContainerRef containerRef = item.getContainerRef();
                item.setContainerSummary(getSummary(containerRef));
                ensureId(item);
                writeItemRef(containerRef, item.toRef());
            }
        }

        @Override
        public WriteResult<Episode> visit(Episode episode) {
            checkArgument(episode.getContainerRef() != null, 
                    "can't write episode with null container");
            
            Episode previous = (Episode) getPreviousContent(episode);
            
            if (previous != null) {
                return writeEpisodeWithExising(episode, previous);
            }
            updateTimes(episode);
            writeRefsAndSummarizeContainers(episode);
            write(episode, NO_PREVIOUS);
            return WriteResult.written(episode).build();
        }

        private WriteResult<Episode> writeEpisodeWithExising(Episode episode, Episode previous) {
            boolean written = false;
            if (hashChanged(episode, previous)) {
                updateWithPevious(episode, previous);
                writeRefsAndSummarizeContainers(episode);
                write(episode, previous);
                written = true;
            } 
            return WriteResult.result(episode, written)
                .withPrevious(previous)
                .build();
        }
        
        private void writeRefsAndSummarizeContainers(Episode episode) {
            ContainerRef primaryContainer = episode.getContainerRef();
            episode.setContainerSummary(getSummary(primaryContainer));

            ItemRef childRef = null;
            if (episode.getSeriesRef() != null) {
                SeriesRef secondaryContainer = episode.getSeriesRef();
                //TODO set series summary on episode
                ContainerSummary summary = getSummary(secondaryContainer);
                ensureId(episode);
                childRef = episode.toRef();
                writeItemRef(secondaryContainer, childRef);
            }
            ensureId(episode);
            childRef = childRef == null ? episode.toRef() : childRef;
            writeItemRef(primaryContainer, childRef);
        }

        @Override
        public WriteResult<Film> visit(Film film) {
            Film previous = (Film) getPreviousContent(film);
            if (previous != null) {
                return writeFilmWithPrevious(film, previous);
            }
            updateTimes(film);
            write(film, NO_PREVIOUS);
            return WriteResult.written(film).build();
        }

        private WriteResult<Film> writeFilmWithPrevious(Film film, Film previous) {
            boolean written = false;
            if (hashChanged(film, previous)) {
                updateWithPevious(film, previous);
                write(film, previous);
                written = true;
            }
            return WriteResult.result(film, written)
                .withPrevious(previous)
                .build();
        }

        @Override
        public WriteResult<Song> visit(Song song) {
            Song previous = (Song) getPreviousContent(song);
            
            if (previous != null) {
                return writeSongWithPrevious(song, previous);
            }
            
            updateTimes(song);
            write(song, NO_PREVIOUS);
            return WriteResult.written(song)
                .build();
        }
        
        private WriteResult<Song> writeSongWithPrevious(Song song, Song previous) {
            boolean written = false;
            if (hashChanged(song, previous)) {
                updateWithPevious(song, previous);
                write(song, previous);
                written = true;
            }
            return WriteResult.result(song, written)
                .withPrevious(previous)
                .build();
        }

        @Override
        public WriteResult<Clip> visit(Clip clip) {
            throw new UnsupportedOperationException("Can't yet write Clips top-level");
        }
    }
    
    private final ContentHasher hasher;
    private final IdGenerator idGenerator;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final Clock clock;

    private final ContentWritingVisitor writingVisitor;
    
    public AbstractContentStore(ContentHasher hasher, IdGenerator idGenerator, MessageSender<ResourceUpdatedMessage> sender, Clock clock) {
        this.hasher = checkNotNull(hasher);
        this.idGenerator = checkNotNull(idGenerator);
        this.clock = checkNotNull(clock);
        this.sender = checkNotNull(sender);
        this.writingVisitor = new ContentWritingVisitor();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <C extends Content> WriteResult<C> writeContent(C content) throws WriteException {
        checkNotNull(content, "write null content");
        checkNotNull(content.getPublisher(), "write unsourced content");
        try {
            WriteResult<C> result = (WriteResult<C>)content.accept(writingVisitor);
            if (result.written()) {
                sendResourceUpdatedMessage(result);
            }
            return result;
        } catch (RuntimeWriteException rwe) {
            throw rwe.getCause();
        }
    }

    private <C extends Content> void sendResourceUpdatedMessage(WriteResult<C> result) {
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

    private Content getPreviousContent(Content c) {
        return resolvePrevious(c.getId(), c.getPublisher(),  c.getAliases());
    }

    protected abstract @Nullable Content resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases);

    private void write(Content content, Content previous) {
        ensureId(content);
        doWriteContent(content, previous);
    }

    private void ensureId(Content content) {
        if(content.getId() == null) {
            content.setId(Id.valueOf(idGenerator.generateRaw()));
        }
    }
    
    protected abstract void doWriteContent(Content content, Content previous);

    private final ContainerSummary getSummary(ContainerRef primary) {
        ContainerSummary summary = summarize(primary);
        if (summary != null) {
            return summary;
        }
        throw new RuntimeWriteException(new MissingResourceException(primary.getId()));
    }
    
    protected abstract ContainerSummary summarize(ContainerRef primary);

    /**
     * Add a ref to the series in the primary container and update its
     * thisOrChildLastUpdated time.
     * 
     * @param primary
     * @param series
     */
    protected abstract void writeSecondaryContainerRef(BrandRef primary, SeriesRef seriesRef);

    /**
     * Add a ref to the child in the container and update its
     * thisOrChildLastUpdated time.
     * 
     * @param containerId
     * @param child
     */
    protected abstract void writeItemRef(ContainerRef containerId, ItemRef childRef);
}
