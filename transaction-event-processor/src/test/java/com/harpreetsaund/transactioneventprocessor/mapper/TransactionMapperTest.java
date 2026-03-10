package com.harpreetsaund.transactioneventprocessor.mapper;

import com.harpreetsaund.transaction.avro.Channel;
import com.harpreetsaund.transaction.avro.Currency;
import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transaction.avro.EventHeaders;
import com.harpreetsaund.transaction.avro.EventPayload;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionType;
import com.harpreetsaund.transactioneventprocessor.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("TransactionMapper Tests")
class TransactionMapperTest {

    private TransactionMapper transactionMapper;

    @BeforeEach
    void setUp() {
        transactionMapper = new TransactionMapper();
    }

    @Test
    @DisplayName("Should map TransactionEvent to Transaction entity successfully")
    void testToTransactionEntityFromTransactionEvent() {
        // Arrange
        String eventId = "evt-123456";
        String sourceSystem = "FILE";
        String topicName = "transactions-topic";
        String transactionId = "txn-001";
        String accountNumber = "ACC-12345";
        TransactionType transactionType = TransactionType.DEBIT;
        double amount = 100.50;
        Currency currency = Currency.CAD;
        String merchantName = "Test Merchant";
        Channel channel = Channel.POS;
        Instant transactionTimestampMs = Instant.now();

        EventHeaders headers = EventHeaders.newBuilder().setEventId(eventId).setSourceSystem(sourceSystem)
                .setTopicName(topicName).setEventType("TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId(transactionId).setAccountNumber(accountNumber)
                .setTransactionType(transactionType).setAmount(amount).setCurrency(currency)
                .setMerchantName(merchantName).setChannel(channel).setTransactionTimestamp(transactionTimestampMs)
                .build();

        TransactionEvent event = TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        // Act
        Transaction transaction = transactionMapper.toTransactionEntity(event);

        // Assert
        assertNotNull(transaction);
        assertEquals(eventId, transaction.getEventId());
        assertEquals(sourceSystem, transaction.getSourceSystem());
        assertEquals(topicName, transaction.getTopicName());
        assertEquals(transactionId, transaction.getTransactionId());
        assertEquals(accountNumber, transaction.getAccountNumber());
        assertEquals(transactionType.name(), transaction.getTransactionType());
        assertEquals(BigDecimal.valueOf(amount), transaction.getAmount());
        assertEquals(currency.name(), transaction.getCurrency());
        assertEquals(merchantName, transaction.getMerchantName());
        assertEquals(channel.name(), transaction.getChannel());
    }

    @Test
    @DisplayName("Should map EodTransactionEvent to Transaction entity successfully")
    void testToTransactionEntityFromEodTransactionEvent() {
        // Arrange
        String eventId = "eod-evt-789";
        String sourceSystem = "MQ";
        String topicName = "eod-transactions-topic";
        String transactionId = "txn-eod-001";
        String accountNumber = "ACC-54321";
        TransactionType transactionType = TransactionType.CREDIT;
        double amount = 250.75;
        Currency currency = Currency.USD;
        String merchantName = "EOD Merchant";
        Channel channel = Channel.ONLINE;
        Instant transactionTimestampMs = Instant.now();

        EventHeaders headers = EventHeaders.newBuilder().setEventId(eventId).setSourceSystem(sourceSystem)
                .setTopicName(topicName).setEventType("EOD_TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId(transactionId).setAccountNumber(accountNumber)
                .setTransactionType(transactionType).setAmount(amount).setCurrency(currency)
                .setMerchantName(merchantName).setChannel(channel).setTransactionTimestamp(transactionTimestampMs)
                .build();

        EodTransactionEvent event = EodTransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        // Act
        Transaction transaction = transactionMapper.toTransactionEntity(event);

        // Assert
        assertNotNull(transaction);
        assertEquals(eventId, transaction.getEventId());
        assertEquals(sourceSystem, transaction.getSourceSystem());
        assertEquals(topicName, transaction.getTopicName());
        assertEquals(transactionId, transaction.getTransactionId());
        assertEquals(accountNumber, transaction.getAccountNumber());
        assertEquals(transactionType.name(), transaction.getTransactionType());
        assertEquals(BigDecimal.valueOf(amount), transaction.getAmount());
        assertEquals(currency.name(), transaction.getCurrency());
        assertEquals(merchantName, transaction.getMerchantName());
        assertEquals(channel.name(), transaction.getChannel());
    }

    @Test
    @DisplayName("Should handle null merchantName in TransactionEvent")
    void testToTransactionEntityWithNullMerchantName() {
        // Arrange
        EventHeaders headers = EventHeaders.newBuilder().setEventId("evt-null-merchant").setSourceSystem("FILE")
                .setTopicName("test-topic").setEventType("TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-null").setAccountNumber("ACC-999")
                .setTransactionType(TransactionType.DEBIT).setAmount(50.0).setCurrency(Currency.CAD)
                .setMerchantName(null).setChannel(Channel.ATM).setTransactionTimestamp(Instant.now()).build();

        TransactionEvent event = TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        // Act
        Transaction transaction = transactionMapper.toTransactionEntity(event);

        // Assert
        assertNotNull(transaction);
        assertNull(transaction.getMerchantName());
        assertEquals("txn-null", transaction.getTransactionId());
    }

    @Test
    @DisplayName("Should map all transaction types correctly")
    void testMapAllTransactionTypes() {
        testTransactionTypeMapping(TransactionType.DEBIT);
        testTransactionTypeMapping(TransactionType.CREDIT);
    }

    @Test
    @DisplayName("Should map all currencies correctly")
    void testMapAllCurrencies() {
        testCurrencyMapping(Currency.CAD);
        testCurrencyMapping(Currency.USD);
        testCurrencyMapping(Currency.EUR);
        testCurrencyMapping(Currency.GBP);
    }

    @Test
    @DisplayName("Should map all channels correctly")
    void testMapAllChannels() {
        testChannelMapping(Channel.ATM);
        testChannelMapping(Channel.POS);
        testChannelMapping(Channel.ONLINE);
        testChannelMapping(Channel.MOBILE);
        testChannelMapping(Channel.ETRANSFER);
    }

    private void testTransactionTypeMapping(TransactionType type) {
        Instant timestamp = Instant.now();
        EventHeaders headers = EventHeaders.newBuilder().setEventId("evt-type-test").setSourceSystem("FILE")
                .setTopicName("test-topic").setEventType("TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-type").setAccountNumber("ACC-001")
                .setTransactionType(type).setAmount(100.0).setCurrency(Currency.CAD).setMerchantName("Test")
                .setChannel(Channel.ONLINE).setTransactionTimestamp(timestamp).build();

        TransactionEvent event = TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        Transaction transaction = transactionMapper.toTransactionEntity(event);

        assertEquals(type.name(), transaction.getTransactionType());
    }

    private void testCurrencyMapping(Currency currency) {
        Instant timestamp = Instant.now();
        EventHeaders headers = EventHeaders.newBuilder().setEventId("evt-currency-test").setSourceSystem("FILE")
                .setTopicName("test-topic").setEventType("TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-currency").setAccountNumber("ACC-001")
                .setTransactionType(TransactionType.DEBIT).setAmount(100.0).setCurrency(currency)
                .setMerchantName("Test").setChannel(Channel.ONLINE).setTransactionTimestamp(timestamp).build();

        TransactionEvent event = TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        Transaction transaction = transactionMapper.toTransactionEntity(event);

        assertEquals(currency.name(), transaction.getCurrency());
    }

    private void testChannelMapping(Channel channel) {
        Instant timestamp = Instant.now();
        EventHeaders headers = EventHeaders.newBuilder().setEventId("evt-channel-test").setSourceSystem("FILE")
                .setTopicName("test-topic").setEventType("TRANSACTION").setEventTimestamp(Instant.now())
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-channel").setAccountNumber("ACC-001")
                .setTransactionType(TransactionType.DEBIT).setAmount(100.0).setCurrency(Currency.CAD)
                .setMerchantName("Test").setChannel(channel).setTransactionTimestamp(timestamp).build();

        TransactionEvent event = TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();

        Transaction transaction = transactionMapper.toTransactionEntity(event);

        assertEquals(channel.name(), transaction.getChannel());
    }
}
