package com.harpreetsaund.mqkafkabridge.config;

import com.harpreetsaund.mqkafkabridge.service.MessageTransformService;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageHandler;

@Configuration
public class OutboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(OutboundChannelConfig.class);

    @Bean
    public DirectChannel outboundKafkaChannel() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel outboundKafkaSuccessChannel() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel outboundKafkaFailureChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundKafkaFlow(MessageTransformService messageTransformService,
            KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        return IntegrationFlow.from("outboundKafkaChannel")
                .transform(messageTransformService, "transformToTransactionEvent")
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate) //
                        .sendSuccessChannel("outboundKafkaSuccessChannel")
                        .sendFailureChannel("outboundKafkaFailureChannel"))
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundKafkaSuccessChannel")
    public MessageHandler outboundKafkaSuccessHandler() {
        return message -> logger.debug("Message sent to Kafka successfully: {}", message.getPayload());
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundKafkaFailureChannel")
    public MessageHandler outboundKafkaFailureHandler() {
        return message -> logger.error("Failed to send message to Kafka: {}", message.getPayload());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Outbound channel configuration enabled.");
    }
}
