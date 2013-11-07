/* Copyright 2009 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.system.bootstrap;

import static com.google.common.base.Predicates.notNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelLister;
import org.atlasapi.media.common.ResourceLister;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentGroupLister;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.people.PeopleLister;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.topic.TopicLister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class ContentBootstrapper {

    private static final String NONE = "NONE";
    private static final String SUCCESS = "SUCCESS";
    private static final String FAIL = "FAIL";
    //
    private static final Logger log = LoggerFactory.getLogger(ContentBootstrapper.class);
    //
    private final ReentrantLock bootstrapLock = new ReentrantLock();
    private final AtomicReference<String> lastStatus = new AtomicReference<String>(NONE);
    private volatile boolean bootstrapping;
    private volatile String destination;
    //

    private ResourceLister<LookupEntry> lookupEntryLister;
    private ContentLister[] contentListers = new ContentLister[0];
    private PeopleLister[] peopleListers = new PeopleLister[0];
    private ChannelLister[] channelListers = new ChannelLister[0];
    private TopicLister[] topicListers = new TopicLister[0];
    private ContentGroupLister[] contentGroupListers = new ContentGroupLister[0];
    
    public ContentBootstrapper withLookupEntryListers(ResourceLister<LookupEntry> lookupEntryLister) {
        this.lookupEntryLister = lookupEntryLister;
        return this;
    }

    public ContentBootstrapper withContentListers(ContentLister... contentListers) {
        this.contentListers = contentListers;
        return this;
    }

    public ContentBootstrapper withPeopleListers(PeopleLister... peopleListers) {
        this.peopleListers = peopleListers;
        return this;
    }

    public ContentBootstrapper withChannelListers(ChannelLister... channelListers) {
        this.channelListers = channelListers;
        return this;
    }

    public ContentBootstrapper withTopicListers(TopicLister... topicListers) {
        this.topicListers = topicListers;
        return this;
    }

    public ContentBootstrapper withContentGroupListers(ContentGroupLister... contentGroupListers) {
        this.contentGroupListers = contentGroupListers;
        return this;
    }

    public boolean loadAllIntoListener(ChangeListener listener) {
        if (bootstrapLock.tryLock()) {
            try {
                Exception error = null;
                bootstrapping = true;
                destination = listener.getClass().toString();
                listener.beforeChange();
                try {
                    
                    if (contentListers.length > 0) {
                        log.info("Bootstrapping containers...");
                        int processedContainers = bootstrapContent(ContentCategory.CONTAINERS, listener);
                        log.info(String.format("Finished bootstrapping %s containers.", processedContainers));
                    }
                    
                    if (contentListers.length > 0) {
                        log.info("Bootstrapping items...");
                        int processedItems = bootstrapContent(ContentCategory.ITEMS, listener);
                        log.info(String.format("Finished bootstrapping %s items.", processedItems));
                    }

                    if (lookupEntryLister != null) {
                        log.info("Bootstrapping lookup entries...");
                        int processedLookupEntries = bootstrapLookupEntries(listener);
                        log.info(String.format("Finished bootstrapping %s lookup entries.", processedLookupEntries));
                    }

                    if (contentGroupListers.length > 0) {
                        log.info("Bootstrapping content groups...");
                        int processedContentGroups = bootstrapContentGroups(listener);
                        log.info(String.format("Finished bootstrapping %s content groups!", processedContentGroups));
                    }

                    if (peopleListers.length > 0) {
                        log.info("Bootstrapping people...");
                        int processedPeople = bootstrapPeople(listener);
                        log.info(String.format("Finished bootstrapping %s people!", processedPeople));
                    }

                    if (channelListers.length > 0) {
                        log.info("Bootstrapping channels...");
                        int processedChannels = bootstrapChannels(listener);
                        log.info(String.format("Finished bootstrapping %s channels!", processedChannels));
                    }

                    if (topicListers.length > 0) {
                        log.info("Bootstrapping topics...");
                        int processedTopics = bootstrapTopics(listener);
                        log.info(String.format("Finished bootstrapping %s topics!", processedTopics));
                    }
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                    error = ex;
                    throw new RuntimeException(ex.getMessage(), ex);
                } finally {
                    if (error == null) {
                        lastStatus.set(SUCCESS);
                    } else {
                        lastStatus.set(FAIL);
                    }
                    listener.afterChange();
                }
            } finally {
                bootstrapping = false;
                bootstrapLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }
    
    public boolean isBootstrapping() {
        return bootstrapping;
    }

    public String getDestination() {
        return destination;
    }

    public String getLastStatus() {
        return lastStatus.get();
    }
    
    private int bootstrapContent(Set<ContentCategory> contentCategories, final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ContentLister lister : contentListers) {
            Iterator<Content> content = lister.listContent(defaultCriteria().forContent(contentCategories).build());
            Iterator<List<Content>> partitionedContent = Iterators.paddedPartition(content, 100);
            while (partitionedContent.hasNext()) {
                List<Content> partition = ImmutableList.copyOf(Iterables.filter(partitionedContent.next(), notNull()));
                listener.onChange(partition);
                processed += partition.size();
                if (log.isInfoEnabled()) {
                    log.info(String.format("%s content processed: %s", processed, ContentListingProgress.progressFrom(Iterables.getLast(partition))));
                }
            }
        }
        return processed;
    }

    private int bootstrapPeople(final ChangeListener listener) {
        final AtomicInteger processed = new AtomicInteger(0);
        for (PeopleLister lister : peopleListers) {
            lister.list(new PeopleListerListener() {

                @Override
                public void personListed(Person person) {
                    listener.onChange(ImmutableList.of(person));
                    processed.incrementAndGet();
                }
            });
        }
        return processed.get();
    }

    private int bootstrapChannels(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ChannelLister lister : channelListers) {
            for (Iterable<Channel> channels : Iterables.partition(lister.all(), 100)) {
                listener.onChange(channels);
                processed += Iterables.size(channels);
            }
        }
        return processed;
    }

    private int bootstrapTopics(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (TopicLister lister : topicListers) {
            for (Iterable<Topic> topics : Iterables.partition(lister.topics(), 100)) {
                listener.onChange(topics);
                processed += Iterables.size(topics);
            }
        }
        return processed;
    }

    private int bootstrapContentGroups(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ContentGroupLister lister : contentGroupListers) {
            for (Iterable<ContentGroup> contentGroups : Iterables.partition(lister.findAll(), 100)) {
                listener.onChange(contentGroups);
                processed += Iterables.size(contentGroups);
            }
        }
        return processed;
    }

    private int bootstrapLookupEntries(final ChangeListener<LookupEntry> listener) throws RuntimeException {
        int processed = 0;
        for (Iterable<LookupEntry> lookupEntries : Iterables.partition(Iterables.filter(lookupEntryLister.list(), Predicates.notNull()), 100)) {
            listener.onChange(lookupEntries);
            processed += Iterables.size(lookupEntries);
            if (processed % 500 == 0) {
                log.info("Lookup entries processed: {}", processed);
            }
        }
        return processed;
    }
    
}
