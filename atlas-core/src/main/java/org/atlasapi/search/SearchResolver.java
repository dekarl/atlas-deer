package org.atlasapi.search;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Identified;

public interface SearchResolver {

    List<Identified> search(SearchQuery query, ApplicationSources sources);
    
}
