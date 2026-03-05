package com.harpreetsaund.transactionfileingestor.mapper;

import com.harpreetsaund.transaction.avro.Channel;
import com.harpreetsaund.transaction.avro.Currency;
import com.harpreetsaund.transaction.avro.TransactionType;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EodTransactionEventMapperTest {

    private EodTransactionEventMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new EodTransactionEventMapper();
        ReflectionTestUtils.setField(mapper, "outboundTopicName", "test-topic");
    }

    @Test
    void toEodTransactionEvent_shouldMapCorrectly() {
        // Given
        EodTransaction eodTransaction = new EodTransaction();
        eodTransaction.setTransactionId("txn123");
        eodTransaction.setAccountNumber("acc123");
        eodTransaction.setType("debit");
        eodTransaction.setAmount("100.50");
        eodTransaction.setCurrency("usd");
        eodTransaction.setTimestamp(1640995200000L); // 2022-01-01 00:00:00 UTC
        eodTransaction.setMerchant("Test Merchant");
        eodTransaction.setChannel("online");

        // When
        var result = mapper.toEodTransactionEvent(eodTransaction);

        // Then
        assertNotNull(result);
        assertNotNull(result.getHeaders());
        assertEquals("test-topic", result.getHeaders().getTopicName());
        assertNotNull(result.getPayload());
        assertEquals("txn123", result.getPayload().getTransactionId());
        assertEquals("acc123", result.getPayload().getAccountNumber());
        assertEquals(TransactionType.DEBIT, result.getPayload().getTransactionType());
        assertEquals(100.50, result.getPayload().getAmount());
        assertEquals(Currency.USD, result.getPayload().getCurrency());
        assertEquals("Test Merchant", result.getPayload().getMerchantName());
        assertEquals(Channel.ONLINE, result.getPayload().getChannel());
    }

    @Test
    void mapTransactionType_debit_shouldReturnDebit() {
        assertEquals(TransactionType.DEBIT, mapper.mapTransactionType("debit"));
        assertEquals(TransactionType.DEBIT, mapper.mapTransactionType("DEBIT"));
    }

    @Test
    void mapTransactionType_credit_shouldReturnCredit() {
        assertEquals(TransactionType.CREDIT, mapper.mapTransactionType("credit"));
        assertEquals(TransactionType.CREDIT, mapper.mapTransactionType("CREDIT"));
    }

    @Test
    void mapTransactionType_null_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapTransactionType(null));
        assertEquals("Transaction type cannot be null", exception.getMessage());
    }

    @Test
    void mapTransactionType_unknown_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapTransactionType("unknown"));
        assertEquals("Unknown transaction type: unknown", exception.getMessage());
    }

    @Test
    void mapCurrency_cad_shouldReturnCad() {
        assertEquals(Currency.CAD, mapper.mapCurrency("cad"));
    }

    @Test
    void mapCurrency_usd_shouldReturnUsd() {
        assertEquals(Currency.USD, mapper.mapCurrency("usd"));
    }

    @Test
    void mapCurrency_eur_shouldReturnEur() {
        assertEquals(Currency.EUR, mapper.mapCurrency("eur"));
    }

    @Test
    void mapCurrency_gbp_shouldReturnGbp() {
        assertEquals(Currency.GBP, mapper.mapCurrency("gbp"));
    }

    @Test
    void mapCurrency_null_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapCurrency(null));
        assertEquals("Currency cannot be null", exception.getMessage());
    }

    @Test
    void mapCurrency_unknown_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapCurrency("unknown"));
        assertEquals("Unknown currency: unknown", exception.getMessage());
    }

    @Test
    void mapChannel_atm_shouldReturnAtm() {
        assertEquals(Channel.ATM, mapper.mapChannel("atm"));
    }

    @Test
    void mapChannel_online_shouldReturnOnline() {
        assertEquals(Channel.ONLINE, mapper.mapChannel("online"));
    }

    @Test
    void mapChannel_pos_shouldReturnPos() {
        assertEquals(Channel.POS, mapper.mapChannel("pos"));
    }

    @Test
    void mapChannel_etransfer_shouldReturnEtransfer() {
        assertEquals(Channel.ETRANSFER, mapper.mapChannel("etransfer"));
    }

    @Test
    void mapChannel_mobile_shouldReturnMobile() {
        assertEquals(Channel.MOBILE, mapper.mapChannel("mobile"));
    }

    @Test
    void mapChannel_null_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapChannel(null));
        assertEquals("Channel cannot be null", exception.getMessage());
    }

    @Test
    void mapChannel_unknown_shouldThrowException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> mapper.mapChannel("unknown"));
        assertEquals("Unknown channel: unknown", exception.getMessage());
    }
}
