package org.atlasapi.system;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentVisitorAdapter;
import org.atlasapi.media.content.ContentWriter;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.util.Identifiables;
import org.atlasapi.media.util.WriteResult;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@Controller
public class IndividualContentBootstrapController {

    private final ContentResolver read;
    private final ContentWriter write;

    public IndividualContentBootstrapController(ContentResolver read, ContentWriter write) {
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
    }
 
    @RequestMapping(value="/system/bootstrap/content", method=RequestMethod.POST)
    public void bootstrapContent(@RequestParam("uri") String uri, HttpServletResponse resp) throws IOException {
        ResolvedContent resolved = read.findByCanonicalUris(ImmutableList.of(uri));
        Identified identified = Iterables.getOnlyElement(resolved.getAllResolvedResults());
        if (!(identified instanceof Content)) {
            resp.sendError(500, "Resolved not content");
            return;
        }
        Content content = (Content) identified;
        String result = content.accept(new ContentVisitorAdapter<String>() {
            
            @Override
            public String visit(Brand brand) {
                WriteResult<Container> brandWrite = write(brand.copy());
                int series = resolveAndWrite(Iterables.transform(brand.getSeriesRefs(), Identifiables.toId()));
                int childs = resolveAndWrite(Iterables.transform(brand.getChildRefs(), Identifiables.toId()));
                return String.format("%s s:%s c:%s", brandWrite, series, childs);
            }
            
            @Override
            public String visit(Series series) {
                WriteResult<Container> seriesWrite = write(series.copy());
                int childs = resolveAndWrite(Iterables.transform(series.getChildRefs(), Identifiables.toId()));
                return String.format("%s c:%s", seriesWrite, childs);
            }

            private int resolveAndWrite(Iterable<Id> ids) {
                List<Identified> resolved = read.findByIds(ids).getAllResolvedResults();
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

            private <C extends Content> WriteResult<C> write(C content) {
                content.setReadHash(null);
                return write.writeContent(content);
            }
            
        });
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(result.length());
        resp.getWriter().println(result);
        resp.getWriter().flush();
    }
    
}
