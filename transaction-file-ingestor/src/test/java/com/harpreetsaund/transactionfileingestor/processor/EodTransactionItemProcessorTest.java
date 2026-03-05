package com.harpreetsaund.transactionfileingestor.processor;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transactionfileingestor.mapper.EodTransactionEventMapper;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EodTransactionItemProcessorTest {

    @Mock
    private EodTransactionEventMapper eodTransactionEventMapper;

    private EodTransactionItemProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new EodTransactionItemProcessor(eodTransactionEventMapper);
    }

    @Test
    void process_withValidItem_shouldReturnMappedEvent() throws Exception {
        // Given
        EodTransaction item = new EodTransaction();
        item.setTransactionId("123");
        EodTransactionEvent expectedEvent = new EodTransactionEvent();
        when(eodTransactionEventMapper.toEodTransactionEvent(item)).thenReturn(expectedEvent);

        // When
        EodTransactionEvent result = processor.process(item);

        // Then
        assertEquals(expectedEvent, result);
        verify(eodTransactionEventMapper).toEodTransactionEvent(item);
    }

    @Test
    void process_withNullItem_shouldReturnNull() throws Exception {
        // When
        EodTransactionEvent result = processor.process(null);

        // Then
        assertNull(result);
        verifyNoInteractions(eodTransactionEventMapper);
    }

    @Test
    void process_withItemHavingNullTransactionId_shouldReturnNull() throws Exception {
        // Given
        EodTransaction item = new EodTransaction();
        item.setTransactionId(null);

        // When
        EodTransactionEvent result = processor.process(item);

        // Then
        assertNull(result);
        verifyNoInteractions(eodTransactionEventMapper);
    }
}
