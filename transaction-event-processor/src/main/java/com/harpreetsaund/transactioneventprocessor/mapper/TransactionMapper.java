package com.harpreetsaund.transactioneventprocessor.mapper;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import com.harpreetsaund.transactioneventprocessor.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TransactionMapper {

    private static final Logger logger = LoggerFactory.getLogger(TransactionMapper.class);

    public Transaction toTransactionEntity(EodTransactionEvent event) {
        logger.debug("Mapping EodTransactionEvent to Transaction entity: {}", event);

        Transaction transaction = new Transaction();
        transaction.setEventId(event.getHeaders().getEventId());
        transaction.setSourceSystem(event.getHeaders().getSourceSystem());
        transaction.setTopicName(event.getHeaders().getTopicName());
        transaction.setTransactionId(event.getPayload().getTransactionId());
        transaction.setAccountNumber(event.getPayload().getAccountNumber());
        transaction.setTransactionType(event.getPayload().getTransactionType().name());
        transaction.setAmount(BigDecimal.valueOf(event.getPayload().getAmount()));
        transaction.setCurrency(event.getPayload().getCurrency().name());
        transaction.setMerchantName(event.getPayload().getMerchantName());
        transaction.setChannel(event.getPayload().getChannel().name());
        transaction.setTransactionTimestamp(event.getPayload().getTransactionTimestamp());

        return transaction;
    }

    public Transaction toTransactionEntity(TransactionEvent event) {
        logger.debug("Mapping TransactionEvent to Transaction entity: {}", event);

        Transaction transaction = new Transaction();
        transaction.setEventId(event.getHeaders().getEventId());
        transaction.setSourceSystem(event.getHeaders().getSourceSystem());
        transaction.setTopicName(event.getHeaders().getTopicName());
        transaction.setTransactionId(event.getPayload().getTransactionId());
        transaction.setAccountNumber(event.getPayload().getAccountNumber());
        transaction.setTransactionType(event.getPayload().getTransactionType().name());
        transaction.setAmount(BigDecimal.valueOf(event.getPayload().getAmount()));
        transaction.setCurrency(event.getPayload().getCurrency().name());
        transaction.setMerchantName(event.getPayload().getMerchantName());
        transaction.setChannel(event.getPayload().getChannel().name());
        transaction.setTransactionTimestamp(event.getPayload().getTransactionTimestamp());

        return transaction;
    }
}
