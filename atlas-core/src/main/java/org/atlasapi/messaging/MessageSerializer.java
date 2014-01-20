package org.atlasapi.messaging;

import com.google.common.io.ByteSource;


public interface MessageSerializer {

    ByteSource serialize(Message msg) throws MessageException;
    
    <M extends Message> M deserialize(ByteSource bytes) throws MessageException;
    
}
