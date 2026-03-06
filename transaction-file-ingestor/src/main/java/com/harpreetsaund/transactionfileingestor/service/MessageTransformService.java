package com.harpreetsaund.transactionfileingestor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
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

    public Message<JobLaunchRequest> transformToBatchJobRequest(Message<File> inboundMessage) {
        logger.debug("Transforming inbound file message {} to batch job launch request", inboundMessage);

        File inboundFile = inboundMessage.getPayload();
        JobParameters jobParameters = new JobParametersBuilder()
                .addJobParameter("input.filepath", inboundFile.getAbsolutePath(), String.class, true)
                .toJobParameters();

        JobLaunchRequest jobLaunchRequest = new JobLaunchRequest(fileToKafkaJob, jobParameters);

        return MessageBuilder.withPayload(jobLaunchRequest).copyHeaders(inboundMessage.getHeaders()).build();
    }
}
