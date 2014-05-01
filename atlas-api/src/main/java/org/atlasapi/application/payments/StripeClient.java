package org.atlasapi.application.payments;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.Application;
import org.atlasapi.application.users.User;
import org.elasticsearch.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.stripe.Stripe;
import com.stripe.exception.APIConnectionException;
import com.stripe.exception.APIException;
import com.stripe.exception.AuthenticationException;
import com.stripe.exception.CardException;
import com.stripe.exception.InvalidRequestException;
import com.stripe.model.Customer;
import com.stripe.model.DeletedCustomer;
import com.stripe.model.Subscription;


public class StripeClient {
    private static Logger log = LoggerFactory.getLogger(StripeClient.class);
    private final String publishableKey;
    
    public StripeClient(String publishableKey, String secretKey) {
        this.publishableKey = checkNotNull(publishableKey);
        Stripe.apiKey = checkNotNull(secretKey);
    }
    
    public Customer createCustomer(User user, Application application) throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        Preconditions.checkArgument(!application.getStripeCustomerId().isPresent());
        Map<String, Object> customerMap = Maps.newHashMap();
        customerMap.put("email", user.getEmail());
        customerMap.put("description", application.getCredentials().getApiKey());
        return Customer.create(customerMap);
    }
    
    public boolean deleteCustomer(Application application) throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        Preconditions.checkArgument(application.getStripeCustomerId().isPresent());
        Customer stripeCustomer = Customer.retrieve(application.getStripeCustomerId().get());
        DeletedCustomer deleted = stripeCustomer.delete();
        return Boolean.TRUE.equals(deleted.getDeleted());
    }
    
    public Customer retrieveCustomer(Application application) throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        Preconditions.checkArgument(application.getStripeCustomerId().isPresent());
        return Customer.retrieve(application.getStripeCustomerId().get());
    }
    
    public Subscription addPlan(Application application, String stripePlanId, String token) throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        Customer customer = retrieveCustomer(application);
        // TODO Check does not already have plan
        Map<String, Object> planMap = Maps.newHashMap();
        planMap.put("plan", stripePlanId);
        planMap.put("token", token);
        return customer.createSubscription(planMap);
    }
    
    public List<Subscription> getSubscriptions(Application application) throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        Customer customer = retrieveCustomer(application);
        Map<String, Object> subscriptionParams = Maps.newHashMap();
        return customer.getSubscriptions().all(subscriptionParams).getData();
    }
}
