package com.harpreetsaund.transactioneventprocessor.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Transaction Model Tests")
class TransactionTest {

    private Transaction transaction;

    @BeforeEach
    void setUp() {
        transaction = new Transaction();
    }

    @Test
    @DisplayName("Should create Transaction with default values")
    void testTransactionCreation() {
        assertNull(transaction.getId());
        assertNull(transaction.getEventId());
        assertNull(transaction.getTransactionId());
        assertNull(transaction.getAccountNumber());
        assertNull(transaction.getTransactionType());
        assertNull(transaction.getAmount());
        assertNull(transaction.getCurrency());
        assertNull(transaction.getMerchantName());
        assertNull(transaction.getChannel());
        assertNull(transaction.getTransactionTimestamp());
        assertNull(transaction.getSourceSystem());
        assertNull(transaction.getTopicName());
    }

    @Test
    @DisplayName("Should set and get all Transaction properties")
    void testTransactionSettersAndGetters() {
        // Arrange
        Long id = 1L;
        String eventId = "evt-123";
        String transactionId = "txn-001";
        String accountNumber = "ACC-12345";
        String transactionType = "DEBIT";
        BigDecimal amount = BigDecimal.valueOf(100.50);
        String currency = "CAD";
        String merchantName = "Test Merchant";
        String channel = "POS";
        Instant transactionTimestamp = Instant.now();
        String sourceSystem = "FILE";
        String topicName = "transactions-topic";
        Instant createdAt = Instant.now();

        // Act
        transaction.setId(id);
        transaction.setEventId(eventId);
        transaction.setTransactionId(transactionId);
        transaction.setAccountNumber(accountNumber);
        transaction.setTransactionType(transactionType);
        transaction.setAmount(amount);
        transaction.setCurrency(currency);
        transaction.setMerchantName(merchantName);
        transaction.setChannel(channel);
        transaction.setTransactionTimestamp(transactionTimestamp);
        transaction.setSourceSystem(sourceSystem);
        transaction.setTopicName(topicName);
        transaction.setCreatedAt(createdAt);

        // Assert
        assertEquals(id, transaction.getId());
        assertEquals(eventId, transaction.getEventId());
        assertEquals(transactionId, transaction.getTransactionId());
        assertEquals(accountNumber, transaction.getAccountNumber());
        assertEquals(transactionType, transaction.getTransactionType());
        assertEquals(amount, transaction.getAmount());
        assertEquals(currency, transaction.getCurrency());
        assertEquals(merchantName, transaction.getMerchantName());
        assertEquals(channel, transaction.getChannel());
        assertEquals(transactionTimestamp, transaction.getTransactionTimestamp());
        assertEquals(sourceSystem, transaction.getSourceSystem());
        assertEquals(topicName, transaction.getTopicName());
        assertEquals(createdAt, transaction.getCreatedAt());
    }

    @Test
    @DisplayName("Should handle null values correctly")
    void testTransactionWithNullValues() {
        // Arrange
        transaction.setEventId("evt-123");
        transaction.setMerchantName(null);
        transaction.setChannel(null);

        // Act & Assert
        assertNull(transaction.getMerchantName());
        assertNull(transaction.getChannel());
        assertEquals("evt-123", transaction.getEventId());
    }

    @Test
    @DisplayName("Should compare two identical transactions as equal")
    void testTransactionEquality() {
        // Arrange
        Instant now = Instant.now();
        transaction.setId(1L);
        transaction.setEventId("evt-123");
        transaction.setTransactionId("txn-001");
        transaction.setAccountNumber("ACC-12345");
        transaction.setTransactionType("DEBIT");
        transaction.setAmount(BigDecimal.valueOf(100.50));
        transaction.setCurrency("CAD");
        transaction.setMerchantName("Test Merchant");
        transaction.setChannel("POS");
        transaction.setTransactionTimestamp(now);
        transaction.setSourceSystem("FILE");
        transaction.setTopicName("topic");
        transaction.setCreatedAt(now);

        Transaction anotherTransaction = new Transaction();
        anotherTransaction.setId(1L);
        anotherTransaction.setEventId("evt-123");
        anotherTransaction.setTransactionId("txn-001");
        anotherTransaction.setAccountNumber("ACC-12345");
        anotherTransaction.setTransactionType("DEBIT");
        anotherTransaction.setAmount(BigDecimal.valueOf(100.50));
        anotherTransaction.setCurrency("CAD");
        anotherTransaction.setMerchantName("Test Merchant");
        anotherTransaction.setChannel("POS");
        anotherTransaction.setTransactionTimestamp(now);
        anotherTransaction.setSourceSystem("FILE");
        anotherTransaction.setTopicName("topic");
        anotherTransaction.setCreatedAt(now);

        // Act & Assert
        assertEquals(transaction, anotherTransaction);
    }

    @Test
    @DisplayName("Should return false when comparing with different transaction")
    void testTransactionInequality() {
        // Arrange
        Instant now = Instant.now();
        transaction.setId(1L);
        transaction.setEventId("evt-123");
        transaction.setAmount(BigDecimal.valueOf(100.50));
        transaction.setTransactionTimestamp(now);

        Transaction differentTransaction = new Transaction();
        differentTransaction.setId(2L);
        differentTransaction.setEventId("evt-456");
        differentTransaction.setAmount(BigDecimal.valueOf(200.75));
        differentTransaction.setTransactionTimestamp(now);

        // Act & Assert
        assertNotEquals(transaction, differentTransaction);
    }

    @Test
    @DisplayName("Should return false when comparing with null")
    void testTransactionEqualityWithNull() {
        // Act & Assert
        assertNotEquals(null, transaction);
    }

    @Test
    @DisplayName("Should generate same hash code for equal objects")
    void testTransactionHashCodeConsistency() {
        // Arrange
        Instant now = Instant.now();
        transaction.setId(1L);
        transaction.setEventId("evt-123");
        transaction.setTransactionId("txn-001");
        transaction.setAmount(BigDecimal.valueOf(100.50));
        transaction.setTransactionTimestamp(now);
        transaction.setCreatedAt(now);

        Transaction anotherTransaction = new Transaction();
        anotherTransaction.setId(1L);
        anotherTransaction.setEventId("evt-123");
        anotherTransaction.setTransactionId("txn-001");
        anotherTransaction.setAmount(BigDecimal.valueOf(100.50));
        anotherTransaction.setTransactionTimestamp(now);
        anotherTransaction.setCreatedAt(now);

        // Act & Assert
        assertEquals(transaction.hashCode(), anotherTransaction.hashCode());
    }

    @Test
    @DisplayName("Should generate different hash codes for different objects")
    void testTransactionHashCodeDifference() {
        // Arrange
        transaction.setId(1L);
        transaction.setEventId("evt-123");

        Transaction differentTransaction = new Transaction();
        differentTransaction.setId(2L);
        differentTransaction.setEventId("evt-456");

        // Act & Assert
        assertNotEquals(transaction.hashCode(), differentTransaction.hashCode());
    }

    @Test
    @DisplayName("Should generate meaningful toString representation")
    void testTransactionToString() {
        // Arrange
        transaction.setId(1L);
        transaction.setEventId("evt-123");
        transaction.setTransactionId("txn-001");
        transaction.setAccountNumber("ACC-12345");

        // Act
        String toStringResult = transaction.toString();

        // Assert
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("evt-123"));
        assertTrue(toStringResult.contains("txn-001"));
        assertTrue(toStringResult.contains("ACC-12345"));
    }

    @Test
    @DisplayName("Should handle large decimal amounts correctly")
    void testTransactionWithLargeAmount() {
        // Arrange
        BigDecimal largeAmount = BigDecimal.valueOf(999999.99);
        transaction.setAmount(largeAmount);

        // Act & Assert
        assertEquals(largeAmount, transaction.getAmount());
        assertEquals(0, largeAmount.compareTo(transaction.getAmount()));
    }

    @Test
    @DisplayName("Should handle all currency codes")
    void testTransactionWithVariousCurrencies() {
        String[] currencies = { "CAD", "USD", "EUR", "GBP" };

        for (String currency : currencies) {
            transaction.setCurrency(currency);
            assertEquals(currency, transaction.getCurrency());
        }
    }

    @Test
    @DisplayName("Should handle all transaction types")
    void testTransactionWithVariousTypes() {
        String[] types = { "DEBIT", "CREDIT" };

        for (String type : types) {
            transaction.setTransactionType(type);
            assertEquals(type, transaction.getTransactionType());
        }
    }

    @Test
    @DisplayName("Should handle all channel types")
    void testTransactionWithVariousChannels() {
        String[] channels = { "ATM", "POS", "ONLINE", "MOBILE", "ETRANSFER" };

        for (String channel : channels) {
            transaction.setChannel(channel);
            assertEquals(channel, transaction.getChannel());
        }
    }

    @Test
    @DisplayName("Should preserve precision in BigDecimal amount")
    void testAmountPrecision() {
        // Arrange
        BigDecimal amount = new BigDecimal("100.50");
        transaction.setAmount(amount);

        // Act & Assert
        assertEquals(2, transaction.getAmount().scale());
        assertEquals("100.50", transaction.getAmount().toPlainString());
    }

    @Test
    @DisplayName("Should handle multiple property updates")
    void testMultiplePropertyUpdates() {
        // Arrange & Act
        transaction.setEventId("evt-1");
        transaction.setEventId("evt-2");
        transaction.setEventId("evt-3");

        transaction.setAmount(BigDecimal.valueOf(100));
        transaction.setAmount(BigDecimal.valueOf(200));
        transaction.setAmount(BigDecimal.valueOf(300));

        // Assert
        assertEquals("evt-3", transaction.getEventId());
        assertEquals(BigDecimal.valueOf(300), transaction.getAmount());
    }

    @Test
    @DisplayName("Should be comparable by event ID in business logic")
    void testTransactionBusinessComparison() {
        // Arrange
        transaction.setEventId("evt-123");
        transaction.setTransactionId("txn-001");

        Transaction another = new Transaction();
        another.setEventId("evt-123");
        another.setTransactionId("txn-002");

        // Act & Assert - Same event ID but different transaction ID
        assertEquals(transaction.getEventId(), another.getEventId());
        assertNotEquals(transaction.getTransactionId(), another.getTransactionId());
    }
}
