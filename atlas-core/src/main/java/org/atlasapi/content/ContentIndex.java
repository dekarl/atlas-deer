package org.atlasapi.content;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public interface ContentIndex {

    ListenableFuture<FluentIterable<Id>> query(AttributeQuerySet query, 
        Iterable<Publisher> publishers, Selection selection);
    
    void index(Content content) throws IndexException;
    
}
