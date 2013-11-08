//package org.atlasapi.output.annotation;
//
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
//import org.atlasapi.persistence.output.UpcomingItemsResolver;
//
//import com.google.common.base.Predicate;
//import com.google.common.base.Predicates;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.metabroadcast.common.ids.NumberToShortStringCodec;
//
//public class UpcomingAnnotation extends OutputAnnotation<Content> {
//
//    private final UpcomingItemsResolver upcomingChildrenResolver;
//    private final ChildRefWriter childRefWriter;
//
//    public UpcomingAnnotation(NumberToShortStringCodec idCodec, UpcomingItemsResolver childrenResolver) {
//        this.upcomingChildrenResolver = childrenResolver;
//        this.childRefWriter = new ChildRefWriter(idCodec, "upcoming");
//    }
//
//    @Override
//    public void write(Content content, FieldWriter writer, OutputContext ctxt) throws IOException {
//        if (content instanceof Container) {
//            Container container = (Container) content;
//            writer.writeList(childRefWriter, Iterables.filter(container.getChildRefs(), upcomingFilter(container)), ctxt);
//        }
//    }
//
//    private Predicate<Identifiable> upcomingFilter(Container container) {
//        Iterable<Id> ids = upcomingChildrenResolver.availableChildrenFor(container);
//        return asChildRefFilter(ids);
//    }
//    
//    private Predicate<Identifiable> asChildRefFilter(Iterable<Id> ids) {
//        return transformingPredicate(Identifiables.toId(), Predicates.in(ImmutableSet.copyOf(ids)));
//    }
//    
//}
