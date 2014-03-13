package org.atlasapi.application.payments;


import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationStore;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.ResourceForbiddenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;



@Controller
public class PaymentsController {
    private static Logger log = LoggerFactory.getLogger(PaymentsController.class);
    private final StripeClient stripeClient;
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;
    private final ApplicationStore applicationStore;

    public PaymentsController(StripeClient stripeClient, 
            NumberToShortStringCodec idCodec,
            ApplicationStore applicationStore,
            UserFetcher userFetcher) {
        this.stripeClient = checkNotNull(stripeClient);
        this.idCodec = idCodec;
        this.applicationStore = applicationStore;
        this.userFetcher = checkNotNull(userFetcher);
    }

    // TODO Add in endpoints to subscribe to source in MBST-8448
    @RequestMapping(value = "/4.0/payments/{aid}/subscribe", method = RequestMethod.POST)
    public void processSUbscribeToken(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String aid,
            @RequestParam String token,
            @RequestParam String planId) throws IOException {
         try {
            response.addHeader("Access-Control-Allow-Origin", "*");
            Id applicationId = Id.valueOf(idCodec.decode(aid));
            if (!userCanAccessApplication(applicationId, request)) {
                throw new ResourceForbiddenException();
            }
            Application existing = applicationStore.applicationFor(applicationId).get();
            stripeClient.addPlan(existing, planId);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        } 
    }
    
    private boolean userCanAccessApplication(Id id, HttpServletRequest request) {
        Optional<User> user = userFetcher.userFor(request);
        if (!user.isPresent()) {
            return false;
        } else {
            return user.get().is(Role.ADMIN) || user.get().getApplicationIds().contains(id);
        }
    }
}
