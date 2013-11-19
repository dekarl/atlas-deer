package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.system.bootstrap.workers.BootstrapContentPersistor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@Controller
public class IndividualContentBootstrapController {

    private final ContentResolver read;
    private final BootstrapContentPersistor write;

    public IndividualContentBootstrapController(ContentResolver read, BootstrapContentPersistor write) {
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
    }
 
    @RequestMapping(value="/system/bootstrap/content", method=RequestMethod.POST)
    public void bootstrapContent(@RequestParam("id") String id, HttpServletResponse resp) throws IOException {
        Identified identified = Iterables.getOnlyElement(resolve(ImmutableList.of(Id.valueOf(id))));
        if (!(identified instanceof Content)) {
            resp.sendError(500, "Resolved not content");
            return;
        }
        Content content = (Content) identified;
        String result = content.accept(new ContentVisitorAdapter<String>() {
            
            @Override
            public String visit(Brand brand) {
                WriteResult<?> brandWrite = write(brand.copy());
                int series = resolveAndWrite(Iterables.transform(brand.getSeriesRefs(), Identifiables.toId()));
                int childs = resolveAndWrite(Iterables.transform(brand.getChildRefs(), Identifiables.toId()));
                return String.format("%s s:%s c:%s", brandWrite, series, childs);
            }
            
            @Override
            public String visit(Series series) {
                WriteResult<?> seriesWrite = write(series.copy());
                int childs = resolveAndWrite(Iterables.transform(series.getChildRefs(), Identifiables.toId()));
                return String.format("%s c:%s", seriesWrite, childs);
            }

            private int resolveAndWrite(Iterable<Id> ids) {
                FluentIterable<Content> resolved = resolve(ids);
                int i = 0;
                for (Content content : Iterables.filter(resolved, Content.class)) {
                    write(content);
                    i++;
                }
                return i;
            }
            
            @Override
            protected String visitItem(Item item) {
                return write(item).toString();
            }

            private WriteResult<? extends Content> write(Content content) {
                content.setReadHash(null);
                return write.writeContent(content);
            }
            
        });
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(result.length());
        resp.getWriter().println(result);
        resp.getWriter().flush();
    }

    private FluentIterable<Content> resolve(Iterable<Id> ids) {
        try {
            ListenableFuture<Resolved<Content>> resolved = read.resolveIds(ids);
            return Futures.get(resolved, IOException.class).getResources();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
    
}
