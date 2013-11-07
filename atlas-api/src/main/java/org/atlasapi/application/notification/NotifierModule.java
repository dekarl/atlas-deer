package org.atlasapi.application.notification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.soy.TemplateRenderer;

@Configuration
public class NotifierModule {
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private @Autowired TemplateRenderer soyRenderer;
    
    private @Value("${notifications.email.host}") String emailHost;
    private @Value("${notifications.email.username}") String emailUsername;
    private @Value("${notifications.email.password}") String emailPassword;
    private @Value("${notifications.email.from}") String from;
    private @Value("${notifications.email.fromFriendlyName}") String fromFriendlyName;
    private @Value("${notifications.email.to}") String to;
    
    @Bean public EmailNotificationSender emailSender() throws Exception {
        JavaMailSenderFactory factory = new JavaMailSenderFactory();
        factory.setHost(emailHost);
        factory.setUsername(emailUsername);
        factory.setPassword(emailPassword);
        
        return EmailNotificationSender.builder()
                .withMailSender(factory.getObject())
                .withRenderer(soyRenderer)
                .withIdCodec(idCodec)
                .withAdminToField(to)
                .withFromField(from)
                .withFriendlyFromName(fromFriendlyName)
                .build();
    }
    
}
