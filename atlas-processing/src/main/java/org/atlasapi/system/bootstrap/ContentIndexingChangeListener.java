package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentIndexingChangeListener extends AbstractMultiThreadedChangeListener<Content> {

    private final Logger log = LoggerFactory.getLogger(ContentIndexingChangeListener.class);
    
    private final ContentIndex contentIndex;

    public ContentIndexingChangeListener(int concurrencyLevel, ContentIndex contentIndex) {
        super(concurrencyLevel);
        this.contentIndex = contentIndex;
    }

    public ContentIndexingChangeListener(ThreadPoolExecutor executor, ContentIndex contentIndex) {
        super(executor);
        this.contentIndex = contentIndex;
    }

    @Override
    protected void onChange(Content change) {
        try {
            contentIndex.index((Content) change);
        } catch (IndexException e) {
            log.error("Failed to index " + change, e);
        }
    }
}
