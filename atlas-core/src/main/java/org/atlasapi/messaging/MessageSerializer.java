package org.atlasapi.messaging;

import com.google.common.io.ByteSource;


public interface MessageSerializer {

    ByteSource serialize(Message msg) throws MessageException;
    
    Message deserialize(ByteSource bytes) throws MessageException;
    
}
