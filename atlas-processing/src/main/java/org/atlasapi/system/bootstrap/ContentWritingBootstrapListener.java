package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.content.Content;
import org.atlasapi.system.bootstrap.workers.BootstrapContentPersistor;

public class ContentWritingBootstrapListener extends AbstractMultiThreadedBootstrapListener<Content> {

    private final BootstrapContentPersistor writer;

    public ContentWritingBootstrapListener(int concurrencyLevel, BootstrapContentPersistor writer) {
        super(concurrencyLevel);
        this.writer = writer;
    }

    public ContentWritingBootstrapListener(ThreadPoolExecutor executor, BootstrapContentPersistor contentStore) {
        super(executor);
        this.writer = contentStore;
    }

    @Override
    protected void onChange(Content content) throws Exception {
        content.setReadHash(null);
        writer.fullWriteContent(content);
    }
}
