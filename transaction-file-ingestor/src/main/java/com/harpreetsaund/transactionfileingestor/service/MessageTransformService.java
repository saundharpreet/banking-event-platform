package com.harpreetsaund.transactionfileingestor.service;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.integration.annotation.Transformer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class MessageTransformService {

    private static final Logger logger = LoggerFactory.getLogger(MessageTransformService.class);

    private final Job fileToKafkaJob;

    public MessageTransformService(Job fileToKafkaJob) {
        this.fileToKafkaJob = fileToKafkaJob;
    }

    @Transformer
    public Message<JobLaunchRequest> transformToBatchJobRequest(Message<File> inboundMessage) {
        logger.debug("Transforming inbound file message {} to batch job launch request", inboundMessage);

        File inboundFile = inboundMessage.getPayload();
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.filepath", inboundFile.getAbsolutePath())
                .toJobParameters();

        JobLaunchRequest jobLaunchRequest = new JobLaunchRequest(fileToKafkaJob, jobParameters);

        return MessageBuilder.withPayload(jobLaunchRequest).copyHeaders(inboundMessage.getHeaders()).build();
    }
}
