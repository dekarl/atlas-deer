package org.atlasapi.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentStore;
import org.atlasapi.topic.TopicStore;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.system.bootstrap.ChangeListener;
import org.atlasapi.system.bootstrap.ContentBootstrapper;
import org.atlasapi.system.bootstrap.ContentIndexingChangeListener;
import org.atlasapi.system.bootstrap.ContentWritingChangeListener;
import org.atlasapi.system.bootstrap.LookupEntryChangeListener;
import org.atlasapi.system.bootstrap.TopicWritingChangeListener;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.collect.Maps;

@Controller
public class BootstrapController {

    private static final Log log = LogFactory.getLog(BootstrapController.class);
    private final ExecutorService scheduler = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private ContentBootstrapper cassandraContentBootstrapper;
    private ContentBootstrapper cassandraLookupEntryBootstrapper;
    private ContentBootstrapper esContentBootstrapper;
    private ContentStore cassandraContentStore;
    private EquivalenceRecordStore equivRecordStore;
    private LookupEntryStore lookupStore;
    private TopicStore cassandraTopicStore;
    private ContentIndex contentIndex;
    private ContentBootstrapper cassandraTopicBootstrapper;

    public void setCassandraContentStore(ContentStore cassandraContentStore) {
        this.cassandraContentStore = cassandraContentStore;
    }
    
    public void setCassandraEquivalenceRecordStore(EquivalenceRecordStore equivRecordStore) {
        this.equivRecordStore = equivRecordStore;
    }
    
    public void setLookupEntryStore(LookupEntryStore lookupStore) {
        this.lookupStore = lookupStore;
        
    }

    public void setCassandraTopicStore(TopicStore cassandraTopicStore) {
        this.cassandraTopicStore = cassandraTopicStore;
    }

    public void setEsContentIndexer(ContentIndex contentIndex) {
        this.contentIndex = contentIndex;
    }

    public void setCassandraContentBootstrapper(ContentBootstrapper cassandraContentBootstrapper) {
        this.cassandraContentBootstrapper = cassandraContentBootstrapper;
    }

    public void setCassandraLookupEntryBootstrapper(ContentBootstrapper cassandraLookupEntryBootstrapper) {
        this.cassandraLookupEntryBootstrapper = cassandraLookupEntryBootstrapper;
    }

    public void setCassandraTopicBootstrapper(ContentBootstrapper cassandraTopicBootstrapper) {
        this.cassandraTopicBootstrapper = cassandraTopicBootstrapper;
    }

    public void setEsContentBootstrapper(ContentBootstrapper esContentBootstrapper) {
        this.esContentBootstrapper = esContentBootstrapper;
    }

    @RequestMapping(method = RequestMethod.POST, value = "/system/bootstrap/cassandra/content")
    public void bootstrapCassandraContent(@RequestParam(required = false) String concurrency, HttpServletResponse response) throws IOException {
        ContentWritingChangeListener changeListener = new ContentWritingChangeListener(getConcurrencyLevel(concurrency, response), cassandraContentStore);
        doBootstrap(cassandraContentBootstrapper, changeListener, response);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/system/bootstrap/cassandra/topic")
    public void bootstrapCassandraTopic(@RequestParam(required = false) String concurrency, HttpServletResponse response) throws IOException {
        TopicWritingChangeListener changeListener = new TopicWritingChangeListener(getConcurrencyLevel(concurrency, response), cassandraTopicStore);
        doBootstrap(cassandraTopicBootstrapper, changeListener, response);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/system/bootstrap/es/content")
    public void bootstrapESContent(@RequestParam(required = false) String concurrency, HttpServletResponse response) throws IOException {
        ContentIndexingChangeListener esChangeListener = new ContentIndexingChangeListener(getConcurrencyLevel(concurrency, response), contentIndex);
        doBootstrap(esContentBootstrapper, esChangeListener, response);
    }
    
    @RequestMapping(method = RequestMethod.POST, value = "/system/bootstrap/cassandra/lookup")
    public void bootstrapCassandraLookupEntries(@RequestParam(required = false) String concurrency, HttpServletResponse response) throws IOException {
        LookupEntryChangeListener changeListener = new LookupEntryChangeListener(getConcurrencyLevel(concurrency, response), lookupStore, equivRecordStore);
        doBootstrap(cassandraLookupEntryBootstrapper, changeListener, response);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/cassandra/content/status")
    public void cassandraContentBootstrapStatus(HttpServletResponse response) throws IOException {
        writeBootstrapStatus(cassandraContentBootstrapper, response);
    }
    
    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/cassandra/lookup/status")
    public void cassandraLookupBootstrapStatus(HttpServletResponse response) throws IOException {
        writeBootstrapStatus(cassandraLookupEntryBootstrapper, response);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/cassandra/topic/status")
    public void cassandraTopicBootstrapStatus(HttpServletResponse response) throws IOException {
        writeBootstrapStatus(cassandraTopicBootstrapper, response);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/es/content/status")
    public void esContentBootstrapStatus(HttpServletResponse response) throws IOException {
        writeBootstrapStatus(esContentBootstrapper, response);
    }

    private void doBootstrap(final ContentBootstrapper contentBootstrapper, final ChangeListener<?> changeListener, HttpServletResponse response) throws IOException {
        try {
            scheduler.submit(new Runnable() {

                @Override
                public void run() {
                    boolean bootstrapping = contentBootstrapper.loadAllIntoListener(changeListener);
                    if (!bootstrapping) {
                        log.warn("Bootstrapping failed because apparently busy bootstrapping something else.");
                    }
                }
            });
        } catch (RejectedExecutionException ex) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Currently bootstrapping another component.");
        }
    }

    public void writeBootstrapStatus(ContentBootstrapper contentBootstrapper, HttpServletResponse response) throws IOException {
        Map<String, Object> result = Maps.newHashMap();
        result.put("bootstrapping", contentBootstrapper.isBootstrapping());
        result.put("lastStatus", contentBootstrapper.getLastStatus());
        if (contentBootstrapper.isBootstrapping()) {
            result.put("destination", contentBootstrapper.getDestination());
        }
        jsonMapper.writeValue(response.getOutputStream(), result);
        response.flushBuffer();
    }

    private int getConcurrencyLevel(String concurrency, HttpServletResponse response) throws IOException {
        int concurrencyLevel = 0;
        if (Strings.isNullOrEmpty(concurrency)) {
            concurrencyLevel = 1;
        } else {
            try {
                concurrencyLevel = Integer.parseInt(concurrency);
            } catch (NumberFormatException ex) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad concurrency parameter!");
            }
        }
        return concurrencyLevel;
    }
}
