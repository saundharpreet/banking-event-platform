package com.harpreetsaund.transactioneventprocessor.config;

import com.harpreetsaund.transactioneventprocessor.model.Transaction;
import jakarta.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.jpa.dsl.Jpa;
import org.springframework.integration.jpa.support.PersistMode;

@Configuration
public class OutboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(OutboundChannelConfig.class);

    @Bean
    public DirectChannel outboundDatabaseChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundDatabaseFlow(EntityManagerFactory entityManagerFactory) {
        return IntegrationFlow.from("outboundDatabaseChannel") //
                .handle(Jpa.outboundAdapter(entityManagerFactory) //
                        .entityClass(Transaction.class) //
                        .persistMode(PersistMode.PERSIST), spec -> spec.transactional(true))
                .log(LoggingHandler.Level.DEBUG).get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Outbound channel configuration enabled.");
    }
}
