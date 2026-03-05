package com.harpreetsaund.transactionfileingestor.config;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transactionfileingestor.processor.EodTransactionEventChunkProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkRequestHandler;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.MessageHandler;

@Configuration
@EnableBatchIntegration
public class BatchIntegrationConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(BatchIntegrationConfig.class);

    @Bean
    public DirectChannel batchJobOutputChannel() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel chunkProcessorRequestChannel() {
        return new DirectChannel();
    }

    @Bean
    public QueueChannel chunkProcessorReplyChannel() {
        return new QueueChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "fileToBatchJobChannel")
    public JobLaunchingGateway jobLaunchingGateway(JobRepository jobRepository) {
        TaskExecutorJobOperator jobOperator = new TaskExecutorJobOperator();
        jobOperator.setTaskExecutor(new SyncTaskExecutor());
        jobOperator.setJobRepository(jobRepository);

        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(jobOperator);
        jobLaunchingGateway.setOutputChannelName("batchJobOutputChannel");

        return jobLaunchingGateway;
    }

    @Bean
    public MessagingTemplate messagingTemplate(DirectChannel chunkProcessorRequestChannel) {
        MessagingTemplate messagingTemplate = new MessagingTemplate();
        messagingTemplate.setDefaultChannel(chunkProcessorRequestChannel);

        return messagingTemplate;
    }

    @Bean
    @ServiceActivator(inputChannel = "chunkProcessorRequestChannel", outputChannel = "chunkProcessorReplyChannel")
    public ChunkProcessorChunkRequestHandler<EodTransactionEvent> chunkProcessorChunkRequestHandler(
            EodTransactionEventChunkProcessor eodTransactionEventChunkProcessor) {
        ChunkProcessorChunkRequestHandler<EodTransactionEvent> chunkProcessorChunkRequestHandler = new ChunkProcessorChunkRequestHandler<>();
        chunkProcessorChunkRequestHandler.setChunkProcessor(eodTransactionEventChunkProcessor);

        return chunkProcessorChunkRequestHandler;
    }

    @Bean
    @ServiceActivator(inputChannel = "batchJobOutputChannel")
    public MessageHandler jobLaunchOutputHandler() {
        return message -> logger.debug("Received job launch response: {}", message);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Batch Integration configuration enabled.");
    }
}
