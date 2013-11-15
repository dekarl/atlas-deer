package org.atlasapi.entity;

import com.google.common.collect.FluentIterable;

public interface ResourceLister<R extends Identifiable> {

    FluentIterable<R> list();
    
}
