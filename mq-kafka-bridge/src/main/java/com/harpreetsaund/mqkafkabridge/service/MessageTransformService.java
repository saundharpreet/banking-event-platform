package com.harpreetsaund.mqkafkabridge.service;

import com.harpreetsaund.mqkafkabridge.mapper.TransactionEventMapper;
import com.harpreetsaund.mqkafkabridge.model.MqTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class MessageTransformService {

    private static final Logger logger = LoggerFactory.getLogger(MessageTransformService.class);

    private final TransactionEventMapper transactionEventMapper;

    public MessageTransformService(TransactionEventMapper transactionEventMapper) {
        this.transactionEventMapper = transactionEventMapper;
    }

    @Transformer
    public Message<MqTransactionEvent> transformToMqTransactionEvent(Message<String> inboundMessage) {
        logger.debug("Transforming inbound message {} to MqTransactionEvent", inboundMessage);

        String payload = inboundMessage.getPayload();
        MqTransactionEvent mqTransactionEvent = transactionEventMapper.toMqTransactionEvent(payload);

        return MessageBuilder.withPayload(mqTransactionEvent).copyHeaders(inboundMessage.getHeaders()).build();
    }

    @Transformer
    public Message<TransactionEvent> transformToTransactionEvent(Message<MqTransactionEvent> inboundMessage) {
        logger.debug("Transforming MqTransactionEvent message {} to TransactionEvent", inboundMessage);

        MqTransactionEvent mqTransactionEvent = inboundMessage.getPayload();
        TransactionEvent transactionEvent = transactionEventMapper.toTransactionEvent(mqTransactionEvent);

        return MessageBuilder.withPayload(transactionEvent).copyHeaders(inboundMessage.getHeaders())
                .setHeader(KafkaHeaders.KEY, transactionEvent.getPayload().getTransactionId()).build();
    }
}
