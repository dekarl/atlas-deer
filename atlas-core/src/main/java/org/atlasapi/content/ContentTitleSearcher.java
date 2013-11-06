package org.atlasapi.content;

import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;

public interface ContentTitleSearcher {
    
    ListenableFuture<SearchResults> search(SearchQuery query);
    
}
