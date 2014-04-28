package org.atlasapi.query.v4.content;

import static org.mockito.Mockito.when;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.Query.SingleQuery;
import org.atlasapi.query.common.QueryContext;
import org.mockito.Mock;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

@Listeners(MockitoTestNGListener.class)
public class IndexBackedEquivalentContentQueryExecutorTest {

    private @Mock ContentIndex contentIndex;
    private @Mock MergingEquivalentsResolver<Content> equivalentContentResolver;
    private IndexBackedEquivalentContentQueryExecutor qe;
    
    @BeforeMethod
    public void setup() {
        qe = new IndexBackedEquivalentContentQueryExecutor(contentIndex, equivalentContentResolver);
    }
    
    @Test(expectedExceptions=NotFoundException.class)
    public void testNoContentForSingleQueryThrowsNotFoundException() throws Exception {
        
        Query<Content> query = Query.singleQuery(Id.valueOf(1), QueryContext.standard());
        QueryContext ctxt = query.getContext();
        
        when(equivalentContentResolver.resolveIds(ImmutableSet.of(query.getOnlyId()), 
                ctxt.getApplicationSources(), ImmutableSet.copyOf(ctxt.getAnnotations().values())))
            .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>empty()));

        qe.execute(query);
        
    }
}
