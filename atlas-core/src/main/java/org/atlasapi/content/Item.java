/* Copyright 2009 British Broadcasting Corporation
 Copyright 2009 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.attribute.AtlasAttribute;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author Lee Denison (lee@metabroadcast.com)
 * @author John Ayres (john@metabroadcast.com)
 */
public class Item extends Content {

    private ContainerRef containerRef;
    private boolean isLongForm = false;
    private Boolean blackAndWhite;
    private Set<Country> countriesOfOrigin = Sets.newHashSet();
    private String sortKey;
    private ContainerSummary containerSummary;
    private Set<Encoding> manifestedAs = Sets.newLinkedHashSet();
    private Set<Broadcast> broadcasts = Sets.newLinkedHashSet();
    private List<SegmentEvent> segmentEvents = ImmutableList.of(); 
    private Set<Restriction> restrictions = Sets.newHashSet();

    public Item(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }
    
    public Item(Id id, Publisher source) {
        super(id, source);
    }

    public Item() {
    }
    
    @Override
    public ItemRef toRef() {
        return new ItemRef(getId(), getPublisher(), SortKey.keyFrom(this), getThisOrChildLastUpdated());
    }

    public void setContainerRef(ContainerRef containerRef) {
        this.containerRef = containerRef;
    }

    public void setContainer(Container container) {
        setContainerRef(container.toRef());
    }

    public ContainerRef getContainerRef() {
        return containerRef;
    }
    public boolean getIsLongForm() {
        return isLongForm;
    }

    public void setIsLongForm(boolean isLongForm) {
        this.isLongForm = isLongForm;
    }


    public Set<Country> getCountriesOfOrigin() {
        return countriesOfOrigin;
    }

    public void setCountriesOfOrigin(Set<Country> countries) {
        this.countriesOfOrigin = Sets.newHashSet();
        for (Country country : countries) {
            countriesOfOrigin.add(country);
        }
    }

    public List<CrewMember> getPeople() {
        return people();
    }

    public void setBlackAndWhite(Boolean blackAndWhite) {
        this.blackAndWhite = blackAndWhite;
    }

    @AtlasAttribute("blackAndWhite")
    public Boolean getBlackAndWhite() {
        return blackAndWhite;
    }
    
   
    @AtlasAttribute("manifestedAs") 
    public Set<Encoding> getManifestedAs() {
        return manifestedAs;
    }

    public void setManifestedAs(Set<Encoding> manifestedAs) {
        this.manifestedAs = manifestedAs;
    }
    
    public void addManifestedAs(Encoding encoding) {
        checkNotNull(encoding);
        manifestedAs.add(encoding);
    }
    
    public Set<Broadcast> getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(Set<Broadcast> broadcasts) {
        this.broadcasts = broadcasts;
    }
    
    public void addBroadcast(Broadcast broadcast) {
        checkNotNull(broadcast);
        broadcasts.add(broadcast);
    }

    public void setRestrictions(Set<Restriction> restrictions) {
        this.restrictions = restrictions;
    }
    
    public void addRestriction(Restriction restriction) {
        checkNotNull(restriction);
        restrictions.add(restriction);
    }

    public Set<Restriction> getRestrictions() {
        return restrictions;
    }
    
    public List<SegmentEvent> getSegmentEvents() {
        return segmentEvents;
    }
    
    public void setSegmentEvents(Iterable<SegmentEvent> segmentEvents) {
        this.segmentEvents = SegmentEvent.ORDERING.immutableSortedCopy(segmentEvents);
    }
    
    public void addSegmentEvents(Iterable<SegmentEvent> segmentEvents) {
        this.segmentEvents = SegmentEvent.ORDERING.immutableSortedCopy(ImmutableSet.<SegmentEvent>builder()
                .addAll(segmentEvents)
                .addAll(this.segmentEvents).build());
    }

    @Override
    public Item copy() {
        Item copy = new Item();
        Item.copyTo(this, copy);
        return copy;
    }

    public static void copyTo(Item from, Item to) {
        Content.copyTo(from, to);
        if (from.containerRef != null) {
            to.containerRef = from.containerRef;
        }
        to.isLongForm = from.isLongForm;
        to.broadcasts = Sets.newHashSet(from.broadcasts);
        to.manifestedAs = Sets.newHashSet(from.manifestedAs);
        to.segmentEvents = Lists.newArrayList(from.segmentEvents);
        to.restrictions = Sets.newHashSet(from.restrictions);
        to.blackAndWhite = from.blackAndWhite;
        to.countriesOfOrigin = Sets.newHashSet(from.countriesOfOrigin);
    }

    public Item withSortKey(String sortKey) {
        this.sortKey = sortKey;
        return this;
    }

    public String sortKey() {
        return sortKey;
    }

    public boolean isChild() {
        return this.containerRef == null;
    }

    public static final Function<Item, Item> COPY = new Function<Item, Item>() {

        @Override
        public Item apply(Item input) {
            return (Item) input.copy();
        }
    };

    public ContainerSummary getContainerSummary() {
        return containerSummary;
    }

    public void setContainerSummary(ContainerSummary containerSummary) {
        this.containerSummary = containerSummary;
    }
    
    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ItemVisitor<V>)visitor);
    }

    @Override
    protected String getSortKey() {
        return SortKey.keyFrom(this);
    }

    public static class ContainerSummary {

        private String type;
        private String title;
        private String description;
        private Integer seriesNumber;

        public ContainerSummary(String type, String title, String description, Integer seriesNumber) {
            this.type = type;
            this.title = title;
            this.description = description;
            this.seriesNumber = seriesNumber;
        }

        private ContainerSummary() {
        }

        public String getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public Integer getSeriesNumber() {
            return seriesNumber;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void setSeriesNumber(Integer seriesNumber) {
            this.seriesNumber = seriesNumber;
        }
    }
    
    public static final Function<Item, Set<Broadcast>> TO_BROADCASTS = new Function<Item, Set<Broadcast>>() {
        @Override
        public Set<Broadcast> apply(Item input) {
            return input.broadcasts;
        }
    };
    
    public static final Function<Item, Set<Encoding>> TO_ENCODINGS = new Function<Item, Set<Encoding>>() {
        @Override
        public Set<Encoding> apply(Item input) {
            return input.manifestedAs;
        }
    };
    
    public static final Function<Item, List<SegmentEvent>> TO_SEGMENT_EVENTS = new Function<Item, List<SegmentEvent>>() {

        @Override
        public List<SegmentEvent> apply(Item input) {
            return input.segmentEvents;
        }
        
    };
    
}
