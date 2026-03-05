package com.harpreetsaund.transactionfileingestor.listener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class BatchJobListenerTest {

    private BatchJobListener listener;

    @Mock
    private JobExecution jobExecution;

    @Mock
    private StepExecution stepExecution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private Chunk<?> chunk;

    @BeforeEach
    void setUp() {
        listener = new BatchJobListener();
    }

    @Test
    void beforeJob_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.beforeJob(jobExecution));
    }

    @Test
    void afterJob_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.afterJob(jobExecution));
    }

    @Test
    void beforeStep_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.beforeStep(stepExecution));
    }

    @Test
    void afterStep_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.afterStep(stepExecution));
    }

    @Test
    void beforeRead_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.beforeRead());
    }

    @Test
    void afterRead_shouldNotThrowException() {
        // Given
        Object item = new Object();

        // When & Then
        assertDoesNotThrow(() -> listener.afterRead(item));
    }

    @Test
    void onReadError_shouldNotThrowException() {
        // Given
        Exception exception = new RuntimeException("Test error");

        // When & Then
        assertDoesNotThrow(() -> listener.onReadError(exception));
    }

    @Test
    void onSkipInRead_shouldNotThrowException() {
        // Given
        Throwable throwable = new RuntimeException("Test skip");

        // When & Then
        assertDoesNotThrow(() -> listener.onSkipInRead(throwable));
    }

    @Test
    void beforeProcess_shouldNotThrowException() {
        // Given
        Object item = new Object();

        // When & Then
        assertDoesNotThrow(() -> listener.beforeProcess(item));
    }

    @Test
    void afterProcess_shouldNotThrowException() {
        // Given
        Object item = new Object();
        Object result = new Object();

        // When & Then
        assertDoesNotThrow(() -> listener.afterProcess(item, result));
    }

    @Test
    void onProcessError_shouldNotThrowException() {
        // Given
        Object item = new Object();
        Exception exception = new RuntimeException("Test process error");

        // When & Then
        assertDoesNotThrow(() -> listener.onProcessError(item, exception));
    }

    @Test
    void onSkipInProcess_shouldNotThrowException() {
        // Given
        Object item = new Object();
        Throwable throwable = new RuntimeException("Test skip process");

        // When & Then
        assertDoesNotThrow(() -> listener.onSkipInProcess(item, throwable));
    }

    @Test
    void beforeWrite_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.beforeWrite(chunk));
    }

    @Test
    void afterWrite_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.afterWrite(chunk));
    }

    @Test
    void onWriteError_shouldNotThrowException() {
        // Given
        Exception exception = new RuntimeException("Test write error");

        // When & Then
        assertDoesNotThrow(() -> listener.onWriteError(exception, chunk));
    }

    @Test
    void onSkipInWrite_shouldNotThrowException() {
        // Given
        Object item = new Object();
        Throwable throwable = new RuntimeException("Test skip write");

        // When & Then
        assertDoesNotThrow(() -> listener.onSkipInWrite(item, throwable));
    }

    @Test
    void beforeChunk_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.beforeChunk(chunk));
    }

    @Test
    void afterChunk_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.afterChunk(chunk));
    }

    @Test
    void onChunkError_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.onChunkError(chunkContext));
    }

    @Test
    void afterChunkError_shouldNotThrowException() {
        // When & Then
        assertDoesNotThrow(() -> listener.afterChunkError(chunkContext));
    }
}
