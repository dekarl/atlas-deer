package org.atlasapi.equiv;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


public class OutputContentMergerTest {

    private final OutputContentMerger merger = new OutputContentMerger();
    
    @Test
    public void testSortOfCommonSourceContentIsStable() {
        Publisher source = Publisher.BBC;
        Brand one = brand(1L, "one",source);
        Brand two = brand(2L, "two",source);
        Brand three = brand(3L, "three",Publisher.TED);
        
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);
        
        ApplicationSources sources = ApplicationSources.defaults()
                .copy().withPrecedence(true)
                .withReadableSources(ImmutableList.of(
                        new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.TED, SourceStatus.AVAILABLE_ENABLED)
                 ))
                .build();
        
        ImmutableList<Brand> contents = ImmutableList.of(one, two, three);
        
        for (List<Brand> contentList : Collections2.permutations(contents)) {
            List<Brand> merged = merger.merge(sources, contentList);
            assertThat(merged.size(), is(1));
            if (contentList.get(0).equals(three)) {
                assertThat(contentList.toString(), merged.get(0), is(contentList.get(1)));
            } else {
                assertThat(contentList.toString(), merged.get(0), is(contentList.get(0)));
            }
        }

    }

    @Test
    public void testMergedContentHasLowestIdOfContentInEquivalenceSet() {
        
        Brand one = brand(5L, "one", Publisher.BBC);
        Brand two = brand(2L, "two",Publisher.PA);
        Brand three = brand(10L, "three",Publisher.TED);
        
        setEquivalent(one, two, three);
        setEquivalent(two, one, three);
        setEquivalent(three, two, one);
        
        //two is intentionally missing here
        ImmutableList<Brand> contents = ImmutableList.of(one, three);

        ApplicationSources sources = ApplicationSources.defaults()
                .copy().withPrecedence(true)
                .withReadableSources(ImmutableList.of(
                        new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.TED, SourceStatus.AVAILABLE_ENABLED)
                 ))
                .build();
        mergePermutations(contents, sources, one, two.getId());

        sources = ApplicationSources.defaults()
                .copy().withPrecedence(true)
                .withReadableSources(ImmutableList.of(
                        new SourceReadEntry(Publisher.TED, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                 ))
                .build();
        mergePermutations(contents, sources, three, two.getId());
        
    }

    private Brand brand(long id, String uri, Publisher source) {
        Brand one = new Brand(uri,uri,source);
        one.setId(id);
        return one;
    }

    private void mergePermutations(ImmutableList<Brand> contents, ApplicationSources sources,
            Brand expectedContent, Id expectedId) {
        for (List<Brand> contentList : Collections2.permutations(contents)) {
            List<Brand> merged = merger.merge(sources, contentList);
            Brand mergedBrand = Iterables.getOnlyElement(merged);
            assertThat(mergedBrand, is(expectedContent));
            assertThat(mergedBrand.getId(), is(expectedId));
        }
    }
    
    private void setEquivalent(Content receiver, Content...equivalents) {
        receiver.setEquivalentTo(ImmutableSet.copyOf(Iterables.transform(
            ImmutableList.copyOf(equivalents), EquivalenceRef.toEquivalenceRef())
        ));
    }
    
}
