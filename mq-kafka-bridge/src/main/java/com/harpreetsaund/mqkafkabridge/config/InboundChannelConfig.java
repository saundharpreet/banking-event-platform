package com.harpreetsaund.mqkafkabridge.config;

import com.harpreetsaund.mqkafkabridge.service.MessageFilterService;
import com.harpreetsaund.mqkafkabridge.service.MessageTransformService;
import jakarta.jms.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.jms.dsl.Jms;

@Configuration
public class InboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(InboundChannelConfig.class);

    @Value("${inbound-channel.queue-name}")
    private String inboundQueueName;

    @Bean
    public IntegrationFlow inboundMqFlow(ConnectionFactory connectionFactory, MessageFilterService messageFilterService,
            MessageTransformService messageTransformService) {
        return IntegrationFlow.from(Jms.messageDrivenChannelAdapter(connectionFactory).destination(inboundQueueName)) //
                .filter(messageFilterService, "filterInboundMessage") //
                .transform(messageTransformService, "transformToMqTransactionEvent") //
                .channel("outboundKafkaChannel").get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Inbound Integration configuration enabled.");
        logger.info("inbound-channel.queue-name={}", inboundQueueName);
    }
}
