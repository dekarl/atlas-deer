package org.atlasapi.system.bootstrap;

public interface ChangeListener<T> {

    void beforeChange();
    
    void onChange(Iterable<? extends T> changed);
        
    void afterChange();
}
