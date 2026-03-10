package com.harpreetsaund.transactioneventprocessor.config;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import com.harpreetsaund.transactioneventprocessor.service.MessageTransformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
public class InboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(InboundChannelConfig.class);

    @Value("${inbound-channel.eod-transaction.topic-name}")
    private String eodTransactionTopicName;

    @Value("${inbound-channel.eod-transaction.group-id}")
    private String eodTransactionGroupId;

    @Value("${inbound-channel.transaction.topic-name}")
    private String transactionTopicName;

    @Value("${inbound-channel.transaction.group-id}")
    private String transactionGroupId;

    @Bean
    public IntegrationFlow eodTransactionFlow(
            ConsumerFactory<String, EodTransactionEvent> eodTransactionEventConsumerFactory,
            MessageTransformService messageTransformService) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(eodTransactionEventConsumerFactory, eodTransactionTopicName)
                        .payloadType(EodTransactionEvent.class) //
                        .configureListenerContainer(configurer -> configurer.concurrency(1) //
                                .groupId(eodTransactionGroupId)))
                .transform(messageTransformService, "eodTransactionEventToTransaction") //
                .channel("outboundDatabaseChannel") //
                .get();
    }

    @Bean
    public IntegrationFlow transactionIntegrationFlow(
            ConsumerFactory<String, TransactionEvent> transactionEventConsumerFactory,
            MessageTransformService messageTransformService) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(transactionEventConsumerFactory, transactionTopicName)
                        .payloadType(TransactionEvent.class) //
                        .configureListenerContainer(configurer -> configurer.concurrency(1) //
                                .groupId(transactionTopicName)))
                .transform(messageTransformService, "transactionEventToTransaction") //
                .channel("outboundDatabaseChannel") //
                .get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Inbound Integration configuration enabled.");
        logger.info("inbound-channel.eod-transaction.topic-name: {}", eodTransactionTopicName);
        logger.info("inbound-channel.eod-transaction.group-id: {}", eodTransactionGroupId);
        logger.info("inbound-channel.transaction.topic-name: {}", transactionTopicName);
        logger.info("inbound-channel.transaction.group-id: {}", transactionGroupId);
    }
}
