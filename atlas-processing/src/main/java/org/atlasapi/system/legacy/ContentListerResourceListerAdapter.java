package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.entity.ResourceLister;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.source.Sources;

import com.google.common.collect.FluentIterable;

public class ContentListerResourceListerAdapter implements ResourceLister<Content> {

    private final ContentLister contentLister;

    public ContentListerResourceListerAdapter(ContentLister contentLister) {
        this.contentLister = contentLister;
    }

    @Override
    public FluentIterable<Content> list() {
        return new FluentIterable<Content>() {
            @Override
            public Iterator<Content> iterator() {
                return contentLister.listContent(ContentListingCriteria.defaultCriteria()
                    .forPublishers(Sources.all().asList())
                    .forContent(
                        ContentCategory.CONTAINER,
                        ContentCategory.PROGRAMME_GROUP,
                        ContentCategory.TOP_LEVEL_ITEM,
                        ContentCategory.CHILD_ITEM
                    )
                    .build());
            }
        };
    }

}
