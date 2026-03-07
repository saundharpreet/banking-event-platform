package com.harpreetsaund.mqkafkabridge.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.annotation.Filter;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class MessageFilterService {

    private final static Logger logger = LoggerFactory.getLogger(MessageFilterService.class);

    @Filter
    public Boolean filterInboundMessage(Message<String> inboundMessage) {
        logger.debug("Filtering inbound message: {}", inboundMessage);

        String payload = inboundMessage.getPayload();
        if (payload.length() == 69 && payload.startsWith("TXN")) {
            logger.debug("Processing inbound message that meets filter criteria: {}", payload);
            return true;
        }

        logger.warn("Skipping inbound message that does not meet filter criteria: {}", payload);
        return false;
    }
}
