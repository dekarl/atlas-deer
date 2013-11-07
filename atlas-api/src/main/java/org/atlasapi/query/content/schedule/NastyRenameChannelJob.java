//package org.atlasapi.query.content.schedule;
//
//import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
//
//import java.util.List;
//import java.util.Set;
//
//import javax.servlet.http.HttpServletResponse;
//
//import org.atlasapi.media.entity.Broadcast;
//import org.atlasapi.media.entity.Channel;
//import org.atlasapi.media.content.Container;
//import org.atlasapi.media.content.Content;
//import org.atlasapi.media.entity.Identified;
//import org.atlasapi.media.entity.Item;
//import org.atlasapi.media.entity.Version;
//import org.atlasapi.persistence.content.ContentWriter;
//import org.atlasapi.persistence.content.RetrospectiveContentLister;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.PathVariable;
//import org.springframework.web.bind.annotation.RequestMapping;
//
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Sets;
//
//@Controller
//public class NastyRenameChannelJob {
//
//    private static final int BATCH_SIZE = 100;
//
//    private final ContentWriter store;
//
//    private final RetrospectiveContentLister lister;
//
//    public NastyRenameChannelJob(RetrospectiveContentLister lister, ContentWriter store) {
//        this.lister = lister;
//        this.store = store;
//    }
//
//    @RequestMapping("/system/rename/{fromChannel}/to/{toChannel}")
//    public void rename(@PathVariable String fromChannel, @PathVariable String toChannel, HttpServletResponse response) {
//        Channel from = Channel.fromKey(fromChannel).requireValue();
//        Channel to = Channel.fromKey(toChannel).requireValue();
//
//        try {
//            String fromId = null;
//            while (true) {
//                List<Content> roots = lister.iterateOverContent(where().fieldEquals("contents.versions.broadcasts.broadcastOn", from.uri()), fromId, -BATCH_SIZE);
//                if (roots.isEmpty()) {
//                    break;
//                }
//
//                for (Content content : roots) {
//                    if (content instanceof Item) {
//                        processItems(ImmutableList.of((Item) content), from, to);
//                    } else if (content instanceof Container<?>) {
//                        processItems(Iterables.filter(((Container<?>) content).getContents(), Item.class), from, to);
//                    }
//                }
//
//                Content last = Iterables.getLast(roots);
//                fromId = last.getCanonicalUri();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//            return;
//        }
//        response.setStatus(HttpServletResponse.SC_OK);
//    }
//
//    private void processItems(Iterable<Item> items, Channel from, Channel to) {
//        for (Item item : items) {
//            try {
//                for (Version version : item.getVersions()) {
//                    Set<Broadcast> broadcasts = Sets.newHashSet();
//
//                    for (Broadcast b : version.getBroadcasts()) {
//                        if (from.uri().equals(b.getBroadcastOn())) {
//                            Broadcast copy = new Broadcast(to.uri(), b.getTransmissionTime(), b.getTransmissionEndTime()).withId(b.getId());
//                            Identified.copyTo(b, copy);
//                            copy.setIsActivelyPublished(b.isActivelyPublished());
//                            copy.setScheduleDate(b.getScheduleDate());
//
//                            broadcasts.add(copy);
//                        } else {
//                            broadcasts.add(b);
//                        }
//                    }
//
//                    version.setBroadcasts(broadcasts);
//                }
//
//                store.createOrUpdate(item);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}
