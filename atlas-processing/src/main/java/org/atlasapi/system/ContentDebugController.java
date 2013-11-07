package org.atlasapi.system;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Type;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentResolver;
import org.atlasapi.media.util.Resolved;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

@Controller
public class ContentDebugController {
    
    private final Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new JsonSerializer<DateTime>() {
        @Override
        public JsonElement serialize(DateTime src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(src.toString());
        }
    }).create();
    
    private final ContentResolver resolver;

    public ContentDebugController(ContentResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @RequestMapping("/system/debug/content/{id}")
    public void printContent(@PathVariable("id") Long id, final HttpServletResponse response) {
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(id));
        Futures.addCallback(resolver.resolveIds(ids), new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                try {
                    Content content = result.getResources().first().orNull();
                    gson.toJson(content, response.getWriter());
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    t.printStackTrace(response.getWriter());
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        });
    }
    
    
}
