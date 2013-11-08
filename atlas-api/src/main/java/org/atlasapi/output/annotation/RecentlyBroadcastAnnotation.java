//package org.atlasapi.output.annotation;
//
//import static com.google.common.base.Preconditions.checkNotNull;
//import static com.metabroadcast.common.base.MorePredicates.transformingPredicate;
//
//import java.io.IOException;
//
//import org.atlasapi.content.Container;
//import org.atlasapi.content.Content;
//import org.atlasapi.entity.Id;
//import org.atlasapi.entity.Identifiable;
//import org.atlasapi.entity.Identifiables;
//import org.atlasapi.output.FieldWriter;
//import org.atlasapi.output.OutputContext;
//import org.atlasapi.output.writers.ChildRefWriter;
//import org.atlasapi.persistence.output.RecentlyBroadcastChildrenResolver;
//
//import com.google.common.base.Predicate;
//import com.google.common.base.Predicates;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.metabroadcast.common.ids.NumberToShortStringCodec;
//
//public class RecentlyBroadcastAnnotation extends OutputAnnotation<Content> {
//
//    private final RecentlyBroadcastChildrenResolver recentlyBroadcastResolver;
//    private final ChildRefWriter childRefWriter;
//
//    public RecentlyBroadcastAnnotation(NumberToShortStringCodec idCodec, RecentlyBroadcastChildrenResolver recentlyBroadcastResolver) {
//        this.recentlyBroadcastResolver = checkNotNull(recentlyBroadcastResolver);
//        this.childRefWriter = new ChildRefWriter(checkNotNull(idCodec), "recent_content");
//    }
//
//    @Override
//    public void write(Content content, FieldWriter writer, OutputContext ctxt) throws IOException {
//        if (content instanceof Container) {
//            Container container = (Container) content;
//            writer.writeList(childRefWriter, Iterables.filter(container.getChildRefs(), recentlyBroadcastFilter(container)), ctxt);
//        }
//    }
//
//    private Predicate<Identifiable> recentlyBroadcastFilter(Container container) {
//        Iterable<Id> ids = recentlyBroadcastResolver.recentlyBroadcastChildrenFor(container, 3);
//        return asChildRefFilter(ids);
//    }
//    
//    private Predicate<Identifiable> asChildRefFilter(Iterable<Id> childRefIds) {
//        return transformingPredicate(Identifiables.toId(), Predicates.in(ImmutableSet.copyOf(childRefIds)));
//    }
//    
//}
