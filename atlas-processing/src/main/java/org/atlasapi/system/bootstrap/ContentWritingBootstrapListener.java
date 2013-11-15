package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;

public class ContentWritingBootstrapListener extends AbstractMultiThreadedBootstrapListener<Content> {

    private final ContentStore contentStore;

    public ContentWritingBootstrapListener(int concurrencyLevel, ContentStore contentStore) {
        super(concurrencyLevel);
        this.contentStore = contentStore;
    }

    public ContentWritingBootstrapListener(ThreadPoolExecutor executor, ContentStore contentStore) {
        super(executor);
        this.contentStore = contentStore;
    }

    @Override
    protected void onChange(Content content) {
        content.setReadHash(null);
        contentStore.writeContent(content);
    }
}
