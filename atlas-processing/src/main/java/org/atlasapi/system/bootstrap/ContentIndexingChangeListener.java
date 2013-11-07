package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentIndexer;
import org.atlasapi.media.content.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentIndexingChangeListener extends AbstractMultiThreadedChangeListener<Content> {

    private final Logger log = LoggerFactory.getLogger(ContentIndexingChangeListener.class);
    
    private final ContentIndexer esContentIndexer;

    public ContentIndexingChangeListener(int concurrencyLevel, ContentIndexer esContentIndexer) {
        super(concurrencyLevel);
        this.esContentIndexer = esContentIndexer;
    }

    public ContentIndexingChangeListener(ThreadPoolExecutor executor, ContentIndexer esContentIndexer) {
        super(executor);
        this.esContentIndexer = esContentIndexer;
    }

    @Override
    protected void onChange(Content change) {
        try {
            esContentIndexer.index((Content) change);
        } catch (IndexException e) {
            log.error("Failed to index " + change, e);
        }
    }
}
