package com.harpreetsaund.transactionfileingestor.mapper;

import com.harpreetsaund.transaction.avro.*;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
public class EodTransactionEventMapper {

    private static final Logger logger = LoggerFactory.getLogger(EodTransactionEventMapper.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'hh:mm:ss'Z'");

    @Value("${outbound-channel.topic-name}")
    private String outboundTopicName;

    public EodTransactionEvent toEodTransactionEvent(EodTransaction eodTransaction) {
        logger.debug("Mapping EodTransaction to EodTransactionEvent: {}", eodTransaction);

        EventHeaders eventHeaders = new EventHeaders();
        eventHeaders.setEventId(UUID.randomUUID().toString());
        eventHeaders.setEventType("EodTransactionEvent");
        eventHeaders.setEventTimestamp(Instant.now());
        eventHeaders.setSourceSystem("SFTP");
        eventHeaders.setTargetSystem("Kafka");
        eventHeaders.setTopicName(outboundTopicName);
        eventHeaders.setPayloadSchemaVersion("1");

        EventPayload eventPayload = new EventPayload();
        eventPayload.setTransactionId(eodTransaction.getTransactionId());
        eventPayload.setAccountNumber(eodTransaction.getAccountNumber());
        eventPayload.setTransactionType(mapTransactionType(eodTransaction.getType()));
        eventPayload.setAmount(new BigDecimal(eodTransaction.getAmount()).doubleValue());
        eventPayload.setCurrency(mapCurrency(eodTransaction.getCurrency()));
        eventPayload.setTransactionTimestamp(Instant.ofEpochMilli(eodTransaction.getTimestamp()));
        eventPayload.setMerchantName(eodTransaction.getMerchant());
        eventPayload.setChannel(mapChannel(eodTransaction.getChannel()));

        EodTransactionEvent eodTransactionEvent = new EodTransactionEvent();
        eodTransactionEvent.setHeaders(eventHeaders);
        eodTransactionEvent.setPayload(eventPayload);

        return eodTransactionEvent;
    }

    public TransactionType mapTransactionType(String transactionType) {
        if (transactionType == null) {
            throw new IllegalArgumentException("Transaction type cannot be null");
        }

        return switch (transactionType.toUpperCase()) {
        case "DEBIT" -> TransactionType.DEBIT;
        case "CREDIT" -> TransactionType.CREDIT;
        default -> throw new IllegalArgumentException("Unknown transaction type: " + transactionType);
        };
    }

    public Currency mapCurrency(String currency) {
        if (currency == null) {
            throw new IllegalArgumentException("Currency cannot be null");
        }

        return switch (currency.toUpperCase()) {
        case "CAD" -> Currency.CAD;
        case "USD" -> Currency.USD;
        case "EUR" -> Currency.EUR;
        case "GBP" -> Currency.GBP;
        default -> throw new IllegalArgumentException("Unknown currency: " + currency);
        };
    }

    public Channel mapChannel(String channel) {
        if (channel == null) {
            throw new IllegalArgumentException("Channel cannot be null");
        }

        return switch (channel.toUpperCase()) {
        case "ATM" -> Channel.ATM;
        case "ONLINE" -> Channel.ONLINE;
        case "POS" -> Channel.POS;
        case "ETRANSFER" -> Channel.ETRANSFER;
        case "MOBILE" -> Channel.MOBILE;
        default -> throw new IllegalArgumentException("Unknown channel: " + channel);
        };
    }
}
