package com.harpreetsaund.transactionfileingestor.model.constants;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EodTransactionFieldNamesTest {

    @Test
    void getFieldNames_shouldReturnAllFieldNames() {
        String[] fieldNames = EodTransactionFieldNames.getFieldNames();

        assertNotNull(fieldNames);
        assertEquals(8, fieldNames.length);
        assertEquals("transactionId", fieldNames[0]);
        assertEquals("accountNumber", fieldNames[1]);
        assertEquals("type", fieldNames[2]);
        assertEquals("amount", fieldNames[3]);
        assertEquals("currency", fieldNames[4]);
        assertEquals("timestamp", fieldNames[5]);
        assertEquals("merchant", fieldNames[6]);
        assertEquals("channel", fieldNames[7]);
    }
}
