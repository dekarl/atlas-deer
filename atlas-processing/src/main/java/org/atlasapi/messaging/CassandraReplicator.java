//package org.atlasapi.messaging;
//
//import java.util.Arrays;
//
//import org.atlasapi.entity.Id;
//import org.atlasapi.content.Container;
//import org.atlasapi.media.entity.Identified;
//import org.atlasapi.media.entity.Item;
//import org.atlasapi.messaging.EntityUpdatedMessage;
//import org.atlasapi.persistence.content.ContentResolver;
//import org.atlasapi.content.ContentWriter;
//import org.atlasapi.persistence.content.ResolvedContent;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// *
// */
//public class CassandraReplicator extends AbstractWorker {
//
//    private final Logger log = LoggerFactory.getLogger(this.getClass());
//    //
//    private final ContentResolver mongoContentResolver;
//    private final ContentWriter cassandraContentWriter;
//
//    public CassandraReplicator(ContentResolver mongoContentResolver, ContentWriter cassandraContentWriter) {
//        this.mongoContentResolver = mongoContentResolver;
//        this.cassandraContentWriter = cassandraContentWriter;
//    }
//
//    @Override
//    public void process(EntityUpdatedMessage message) {
//        // TODO_SB : ResolvedContent should be eliminated or made way simpler:
//        ResolvedContent results = mongoContentResolver.findByIds(Arrays.asList(Id.valueOf(message.getEntityId())));
//        //
//        if (results.getAllResolvedResults().size() > 1) {
//            throw new IllegalStateException("More than one content found for id: " + message.getEntityId());
//        } else if (results.getAllResolvedResults().size() == 1) {
//            Identified source = results.getFirstValue().requireValue();
//            if (source instanceof Container) {
//                cassandraContentWriter.createOrUpdate((Container) source);
//            } else if (source instanceof Item) {
//                cassandraContentWriter.createOrUpdate((Item) source);
//            } else {
//                log.warn("Unexpected type {} found for id {} on message of type {} and id {}.",
//                        new Object[]{source.getClass().getName(), message.getEntityId(), message.getClass().getName(), message.getMessageId()});
//            }
//        } else {
//            log.warn("No content found for id {} on message of type {} and id {}.",
//                    new Object[]{message.getEntityId(), message.getClass().getName(), message.getMessageId()});
//        }
//    }
//}
