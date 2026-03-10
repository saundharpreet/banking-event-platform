package com.harpreetsaund.transactioneventprocessor.service;

import com.harpreetsaund.transaction.avro.Channel;
import com.harpreetsaund.transaction.avro.Currency;
import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transaction.avro.EventHeaders;
import com.harpreetsaund.transaction.avro.EventPayload;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionType;
import com.harpreetsaund.transactioneventprocessor.mapper.TransactionMapper;
import com.harpreetsaund.transactioneventprocessor.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("MessageTransformService Tests")
@ExtendWith(MockitoExtension.class)
class MessageTransformServiceTest {

    @Mock
    private TransactionMapper transactionMapper;

    private MessageTransformService messageTransformService;

    @BeforeEach
    void setUp() {
        messageTransformService = new MessageTransformService(transactionMapper);
    }

    @Test
    @DisplayName("Should transform EodTransactionEvent message to Transaction message")
    void testEodTransactionEventToTransactionTransformation() {
        // Arrange
        EodTransactionEvent eodEvent = createEodTransactionEvent();
        Message<EodTransactionEvent> inboundMessage = MessageBuilder.withPayload(eodEvent)
                .setHeader("test-header", "test-value").build();

        Transaction expectedTransaction = createTransaction();
        when(transactionMapper.toTransactionEntity(eodEvent)).thenReturn(expectedTransaction);

        // Act
        Message<Transaction> result = messageTransformService.eodTransactionEventToTransaction(inboundMessage);

        // Assert
        assertNotNull(result);
        assertEquals(expectedTransaction, result.getPayload());
        assertEquals("test-value", result.getHeaders().get("test-header"));
        verify(transactionMapper, times(1)).toTransactionEntity(eodEvent);
    }

    @Test
    @DisplayName("Should transform TransactionEvent message to Transaction message")
    void testTransactionEventToTransactionTransformation() {
        // Arrange
        TransactionEvent transactionEvent = createTransactionEvent();
        Message<TransactionEvent> inboundMessage = MessageBuilder.withPayload(transactionEvent).build();

        Transaction expectedTransaction = createTransaction();
        when(transactionMapper.toTransactionEntity(transactionEvent)).thenReturn(expectedTransaction);

        // Act
        Message<Transaction> result = messageTransformService.transactionEventToTransaction(inboundMessage);

        // Assert
        assertNotNull(result);
        assertEquals(expectedTransaction, result.getPayload());
        verify(transactionMapper, times(1)).toTransactionEntity(transactionEvent);
    }

    @Test
    @DisplayName("Should preserve all message headers during EodTransactionEvent transformation")
    void testEodTransactionEventPreservesAllHeaders() {
        // Arrange
        EodTransactionEvent eodEvent = createEodTransactionEvent();
        Message<EodTransactionEvent> inboundMessage = MessageBuilder.withPayload(eodEvent)
                .setHeader("header1", "value1").setHeader("header2", "value2").setHeader("header3", 123).build();

        Transaction expectedTransaction = createTransaction();
        when(transactionMapper.toTransactionEntity(eodEvent)).thenReturn(expectedTransaction);

        // Act
        Message<Transaction> result = messageTransformService.eodTransactionEventToTransaction(inboundMessage);

        // Assert
        assertEquals("value1", result.getHeaders().get("header1"));
        assertEquals("value2", result.getHeaders().get("header2"));
        assertEquals(123, result.getHeaders().get("header3"));
    }

    @Test
    @DisplayName("Should return Transaction message with correct payload type")
    void testReturnMessageHasCorrectPayloadType() {
        // Arrange
        TransactionEvent transactionEvent = createTransactionEvent();
        Message<TransactionEvent> inboundMessage = MessageBuilder.withPayload(transactionEvent).build();

        Transaction expectedTransaction = createTransaction();
        when(transactionMapper.toTransactionEntity(transactionEvent)).thenReturn(expectedTransaction);

        // Act
        Message<Transaction> result = messageTransformService.transactionEventToTransaction(inboundMessage);

        // Assert
        assertNotNull(result);
        result.getPayload();
        assertEquals(Transaction.class, result.getPayload().getClass());
    }

    @Test
    @DisplayName("Should handle empty message headers")
    void testHandleMessageWithoutCustomHeaders() {
        // Arrange
        TransactionEvent transactionEvent = createTransactionEvent();
        Message<TransactionEvent> inboundMessage = MessageBuilder.withPayload(transactionEvent).build();

        Transaction expectedTransaction = createTransaction();
        when(transactionMapper.toTransactionEntity(transactionEvent)).thenReturn(expectedTransaction);

        // Act
        Message<Transaction> result = messageTransformService.transactionEventToTransaction(inboundMessage);

        // Assert
        assertNotNull(result);
        assertNotNull(result.getHeaders());
        assertEquals(expectedTransaction, result.getPayload());
    }

    private EodTransactionEvent createEodTransactionEvent() {
        Instant timestamp = Instant.now();
        EventHeaders headers = EventHeaders.newBuilder().setEventId("eod-evt-001").setSourceSystem("FILE")
                .setTopicName("eod-topic").setEventType("EOD_TRANSACTION").setEventTimestamp(timestamp)
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-eod-001").setAccountNumber("ACC-12345")
                .setTransactionType(TransactionType.DEBIT).setAmount(100.50).setCurrency(Currency.CAD)
                .setMerchantName("Test Merchant").setChannel(Channel.POS).setTransactionTimestamp(timestamp).build();

        return EodTransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();
    }

    private TransactionEvent createTransactionEvent() {
        Instant timestamp = Instant.now();
        EventHeaders headers = EventHeaders.newBuilder().setEventId("evt-001").setSourceSystem("MQ")
                .setTopicName("transaction-topic").setEventType("TRANSACTION").setEventTimestamp(timestamp)
                .setPayloadSchemaVersion("1.0").build();

        EventPayload payload = EventPayload.newBuilder().setTransactionId("txn-001").setAccountNumber("ACC-54321")
                .setTransactionType(TransactionType.CREDIT).setAmount(250.75).setCurrency(Currency.USD)
                .setMerchantName("Online Merchant").setChannel(Channel.ONLINE).setTransactionTimestamp(timestamp)
                .build();

        return TransactionEvent.newBuilder().setHeaders(headers).setPayload(payload).build();
    }

    private Transaction createTransaction() {
        Transaction transaction = new Transaction();
        transaction.setEventId("evt-001");
        transaction.setTransactionId("txn-001");
        transaction.setAccountNumber("ACC-54321");
        transaction.setTransactionType("CREDIT");
        transaction.setAmount(BigDecimal.valueOf(250.75));
        transaction.setCurrency("USD");
        transaction.setMerchantName("Online Merchant");
        transaction.setChannel("ONLINE");
        transaction.setTransactionTimestamp(Instant.now());
        transaction.setSourceSystem("MQ");
        transaction.setTopicName("transaction-topic");
        return transaction;
    }
}
