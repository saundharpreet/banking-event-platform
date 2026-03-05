package com.harpreetsaund.transactionfileingestor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageTransformServiceTest {

    @Mock
    private Job fileToKafkaJob;

    private MessageTransformService service;

    @BeforeEach
    void setUp() {
        service = new MessageTransformService(fileToKafkaJob);
    }

    @Test
    void transformToBatchJobRequest_shouldCreateJobLaunchRequest() {
        // Given
        File testFile = new File("/path/to/test/file.txt");
        Message<File> inboundMessage = MessageBuilder.withPayload(testFile).build();

        // When
        Message<JobLaunchRequest> result = service.transformToBatchJobRequest(inboundMessage);

        // Then
        assertNotNull(result);
        assertNotNull(result.getPayload());

        JobLaunchRequest jobLaunchRequest = result.getPayload();
        assertEquals(fileToKafkaJob, jobLaunchRequest.getJob());

        JobParameters jobParameters = jobLaunchRequest.getJobParameters();
        assertNotNull(jobParameters);
        assertEquals("/path/to/test/file.txt", jobParameters.getString("input.filepath"));
    }
}
