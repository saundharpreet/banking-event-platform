package com.harpreetsaund.transactioneventprocessor.service;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import com.harpreetsaund.transactioneventprocessor.mapper.TransactionMapper;
import com.harpreetsaund.transactioneventprocessor.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class MessageTransformService {

    private static final Logger logger = LoggerFactory.getLogger(MessageTransformService.class);

    private final TransactionMapper transactionMapper;

    public MessageTransformService(TransactionMapper transactionMapper) {
        this.transactionMapper = transactionMapper;
    }

    @Transformer
    public Message<Transaction> eodTransactionEventToTransaction(Message<EodTransactionEvent> inboundMessage) {
        logger.debug("Transforming EodTransactionEvent to Transaction entity: {}", inboundMessage);

        EodTransactionEvent eodTransactionEvent = inboundMessage.getPayload();
        Transaction transaction = transactionMapper.toTransactionEntity(eodTransactionEvent);

        return MessageBuilder.withPayload(transaction).copyHeaders(inboundMessage.getHeaders()).build();
    }

    @Transformer
    public Message<Transaction> transactionEventToTransaction(Message<TransactionEvent> inboundMessage) {
        logger.debug("Transforming TransactionEvent to Transaction entity: {}", inboundMessage);

        TransactionEvent transactionEvent = inboundMessage.getPayload();
        Transaction transaction = transactionMapper.toTransactionEntity(transactionEvent);

        return MessageBuilder.withPayload(transaction).copyHeaders(inboundMessage.getHeaders()).build();
    }
}
