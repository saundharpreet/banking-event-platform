package com.harpreetsaund.transactionfileingestor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(OutboundChannelConfig.class);

    @Value("${outbound-channel.topic-name}")
    private String outboundTopicName;

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Outbound channel configuration enabled.");
        logger.info("outbound-channel.topic-name: {}", outboundTopicName);
    }
}
