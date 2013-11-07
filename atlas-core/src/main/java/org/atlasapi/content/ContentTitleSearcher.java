package org.atlasapi.content;

import com.google.common.util.concurrent.ListenableFuture;

import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResults;

public interface ContentTitleSearcher {
    
    ListenableFuture<SearchResults> search(SearchQuery query);
    
}
