package org.atlasapi.content;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.atlasapi.PersistenceModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;

public abstract class EquivalentContentStoreTestSuite {
    
    private EquivalentContentStore store;
    private ContentStore contentStore;
    private EquivalenceGraphStore graphStore;

    abstract PersistenceModule persistenceModule() throws Exception;
    
    @BeforeClass
    public void setup() throws Exception {
        PersistenceModule module = persistenceModule();
        this.store = module.equivalentContentStore();
        this.contentStore = module.contentStore();
        this.graphStore = module.contentEquivalenceGraphStore();
    }
    
    @Test
    public void testWritingContentThatDoesntYetHaveAGraph() throws Exception {
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content> result = contentStore.writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        
        store.updateContent(content.toRef());
        
        ResolvedEquivalents<Content> resolved
            = get(store.resolveIds(ImmutableList.of(content.getId()), ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content.getId());
        assertThat(Iterables.getOnlyElement(set), is(content));
    }

    private <T> T get(ListenableFuture<T> resolveIds) throws Exception {
        return Futures.get(resolveIds, 1, TimeUnit.MINUTES, Exception.class);
    }

    @Test
    public void testWritingAnEquivalenceGraph() throws Exception {
        Content content2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Two");

        Content content3 = new Item(Id.valueOf(3), Publisher.BBC);
        content3.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content3.setTitle("Three");
        
        contentStore.writeContent(content2);
        contentStore.writeContent(content3);
        
        Optional<ImmutableSet<EquivalenceGraph>> graphs
            = graphStore.updateEquivalences(content2.toRef(), ImmutableSet.<ResourceRef>of(content3.toRef()), 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        store.updateEquivalences(graphs.get());
        
        ResolvedEquivalents<Content> resolved
            = get(store.resolveIds(ImmutableList.of(content2.getId()), ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC), Annotation.all()));
    
        ImmutableSet<Content> set = resolved.get(content2.getId());
        assertThat(set, hasSize(2));
        assertThat(set, hasItems(content2, content3));
    }
    
}
