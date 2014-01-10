package org.atlasapi.schedule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.util.WriteResult;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Convenience class to transform a number of schedule hierarchies into unique groups for writing.  
 */
class WritableScheduleHierarchy {
    
    abstract class NestableContent {
        
        abstract Content getContent();
        
        abstract ImmutableSet<NestableContent> getNestedContent();
        
    }

    static WritableScheduleHierarchy from(List<ScheduleHierarchy> hierarchies) {
        ImmutableSet.Builder<Container> topLevelContainers = ImmutableSet.builder();
        ImmutableSet.Builder<Series> series = ImmutableSet.builder();
        Map<Series, Brand> seriesBrandIndex = Maps.newHashMap();
        ImmutableSet.Builder<Item> items = ImmutableSet.builder();
        Map<Item, Container> itemPrimaryContainerIndex = Maps.newHashMap();
        Map<Item, Series> itemSeriesIndex = Maps.newHashMap();
        
        for (ScheduleHierarchy hierarchy : hierarchies) {
            Optional<Container> tlc = hierarchy.getPrimaryContainer();
            if (tlc.isPresent()) {
                topLevelContainers.add(tlc.get());
            }
            Optional<Series> possibleSeries = hierarchy.getPossibleSeries();
            if (possibleSeries.isPresent()) {
                series.add(possibleSeries.get());
                if (tlc.isPresent()) {
                    seriesBrandIndex.put(possibleSeries.get(), (Brand) tlc.get());
                }
            }
            Item item = hierarchy.getItemAndBroadcast().getItem();
            items.add(item);
            if (tlc.isPresent()) {
                itemPrimaryContainerIndex.put(item, tlc.get());
            }
            if (possibleSeries.isPresent()) {
                itemSeriesIndex.put(item, possibleSeries.get());
            }
        }
        return new WritableScheduleHierarchy(topLevelContainers.build(), series.build(), items.build(), seriesBrandIndex, itemPrimaryContainerIndex, itemSeriesIndex);
    }
    
    private Set<Container> topLevelContainers;
    private Set<Series> series;
    private Set<Item> items;
    private Map<Series, Brand> seriesBrandIndex;
    private Map<Item, Container> itemPrimaryContainerIndex;
    private Map<Item, Series> itemSeriesIndex;

    public WritableScheduleHierarchy(Set<Container> topLevelContainers, Set<Series> series,
            Set<Item> items, Map<Series, Brand> seriesBrandIndex,
            Map<Item, Container> itemPrimaryContainerIndex, Map<Item, Series> itemSeriesIndex) {
        this.topLevelContainers = topLevelContainers;
        this.series = series;
        this.items = items;
        this.seriesBrandIndex = seriesBrandIndex;
        this.itemPrimaryContainerIndex = itemPrimaryContainerIndex;
        this.itemSeriesIndex = itemSeriesIndex;
    }

    List<WriteResult<? extends Content>> writeTo(ContentStore contentStore) {
        ImmutableList.Builder<WriteResult<? extends Content>> results = ImmutableList.builder();
        Map<Container, Container> tlcWritten = writePrimaryContainers(contentStore, results);
        Map<Series, Series> seriesWritten
            = writeSecondaryContainers(contentStore, results, tlcWritten);
        writeItems(contentStore, results, tlcWritten, seriesWritten);
        return results.build();
    }
    
    private Map<Container, Container> writePrimaryContainers(ContentStore contentStore,
            ImmutableList.Builder<WriteResult<? extends Content>> results) {
        Map<Container, Container> tlcWritten = Maps.newHashMap();
        for (Container container : topLevelContainers) {
            WriteResult<Container> written = contentStore.writeContent(container);
            results.add(written);
            tlcWritten.put(container, written.getResource());
        }
        return tlcWritten;
    }
    
    private Map<Series, Series> writeSecondaryContainers(ContentStore contentStore,
            ImmutableList.Builder<WriteResult<? extends Content>> results,
            Map<Container, Container> tlcWritten) {
        Map<Series, Series> seriesWritten = Maps.newHashMap();
        for (Series sery : series) {
            Brand tlc = seriesBrandIndex.get(sery);
            if (tlc != null) {
                sery.setParent((Brand) tlcWritten.get(tlc));
            }
            WriteResult<Series> written = contentStore.writeContent(sery);
            results.add(written);
            seriesWritten.put(sery, written.getResource());
        }
        return seriesWritten;
    }
    

    private void writeItems(ContentStore contentStore,
            ImmutableList.Builder<WriteResult<? extends Content>> results,
            Map<Container, Container> tlcWritten, Map<Series, Series> seriesWritten) {
        for (Item item : items) {
            Container tlc = itemPrimaryContainerIndex.get(item);
            if (tlc != null) {
                item.setContainer(tlcWritten.get(tlc));
            }
            if (item instanceof Episode) {
                Series series = itemSeriesIndex.get(item);
                if (series != null) {
                    ((Episode)item).setSeries(seriesWritten.get(series));
                }
            }
            results.add(contentStore.writeContent(item));
        }
    }

}
