package com.harpreetsaund.mqkafkabridge.mapper;

import com.harpreetsaund.mqkafkabridge.model.MqTransactionEvent;
import com.harpreetsaund.transaction.avro.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
public class TransactionEventMapper {

    private static final Logger logger = LoggerFactory.getLogger(TransactionEventMapper.class);

    private static final DateTimeFormatter INBOUND_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    @Value("${outbound-channel.topic-name}")
    private String outboundTopicName;

    public MqTransactionEvent toMqTransactionEvent(String payload) {
        logger.debug("Mapping payload {} to MqTransactionEvent", payload);

        MqTransactionEvent mqTransactionEvent = new MqTransactionEvent();
        mqTransactionEvent.setRecordType(payload.substring(0, 3).trim());
        mqTransactionEvent.setTransactionId(payload.substring(3, 13).trim());
        mqTransactionEvent.setAccountNumber(payload.substring(13, 23).trim());
        mqTransactionEvent.setTransactionType(payload.substring(23, 24));
        mqTransactionEvent.setAmount(new BigDecimal(payload.substring(24, 34).trim()));
        mqTransactionEvent.setCurrency(payload.substring(34, 37).trim());
        mqTransactionEvent.setTimestamp(payload.substring(37, 51).trim());
        mqTransactionEvent.setMerchantName(payload.substring(51, 66).trim());
        mqTransactionEvent.setChannel(payload.substring(66, 69).trim());

        return mqTransactionEvent;
    }

    public TransactionEvent toTransactionEvent(MqTransactionEvent mqTransactionEvent) {
        logger.debug("Mapping MqTransactionEvent {} to TransactionEvent", mqTransactionEvent);

        EventHeaders eventHeaders = new EventHeaders();
        eventHeaders.setEventId(UUID.randomUUID().toString());
        eventHeaders.setEventType("EodTransactionEvent");
        eventHeaders.setEventTimestamp(Instant.now());
        eventHeaders.setSourceSystem("SFTP");
        eventHeaders.setTargetSystem("Kafka");
        eventHeaders.setTopicName(outboundTopicName);
        eventHeaders.setPayloadSchemaVersion("1");

        EventPayload eventPayload = new EventPayload();
        eventPayload.setTransactionId(mqTransactionEvent.getTransactionId());
        eventPayload.setAccountNumber(mqTransactionEvent.getAccountNumber());
        eventPayload.setTransactionType(mapTransactionType(mqTransactionEvent.getTransactionType()));
        eventPayload.setAmount(mqTransactionEvent.getAmount().doubleValue());
        eventPayload.setCurrency(mapCurrency(mqTransactionEvent.getCurrency()));
        eventPayload.setTransactionTimestamp(mapTimestamp(mqTransactionEvent.getTimestamp()));
        eventPayload.setMerchantName(mqTransactionEvent.getMerchantName());
        eventPayload.setChannel(mapChannel(mqTransactionEvent.getChannel()));

        TransactionEvent transactionEvent = new TransactionEvent();
        transactionEvent.setHeaders(eventHeaders);
        transactionEvent.setPayload(eventPayload);

        return transactionEvent;
    }

    public TransactionType mapTransactionType(String transactionType) {
        if (transactionType == null) {
            throw new IllegalArgumentException("Transaction type cannot be null");
        }

        return switch (transactionType.toUpperCase()) {
        case "D" -> TransactionType.DEBIT;
        case "C" -> TransactionType.CREDIT;
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

    public Instant mapTimestamp(String timestamp) {
        LocalDateTime dateTime = LocalDateTime.parse(timestamp, INBOUND_DATETIME_FORMATTER);
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    public Channel mapChannel(String channel) {
        if (channel == null) {
            throw new IllegalArgumentException("Channel cannot be null");
        }

        return switch (channel.toUpperCase()) {
        case "ATM" -> Channel.ATM;
        case "POS" -> Channel.POS;
        case "ONL" -> Channel.ONLINE;
        case "MOB" -> Channel.MOBILE;
        case "ETR" -> Channel.ETRANSFER;
        default -> throw new IllegalArgumentException("Unknown channel: " + channel);
        };
    }
}
