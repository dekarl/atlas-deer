package org.atlasapi.content;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;

import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;


public abstract class ForwardingContentStore implements ContentStore {

    protected ForwardingContentStore() { }
    
    protected abstract ContentStore delegate();
    
    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        return delegate().resolveIds(ids);
    }

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        return delegate().resolveAliases(aliases, source);
    }

    @Override
    public <C extends Content> WriteResult<C> writeContent(C content) {
        return delegate().writeContent(content);
    }

}
