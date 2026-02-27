package com.harpreetsaund.transactionfileingestor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;

@Configuration
@EnableIntegration
public class IntegrationConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationConfig.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Spring Integration configuration enabled.");
    }
}
