package com.harpreetsaund.transactionfileingestor.config;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transactionfileingestor.listener.BatchJobListener;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import com.harpreetsaund.transactionfileingestor.model.constants.EodTransactionFieldNames;
import com.harpreetsaund.transactionfileingestor.processor.EodTransactionItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.FieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.infrastructure.item.file.transform.IncorrectTokenCountException;
import org.springframework.batch.infrastructure.item.file.transform.LineTokenizer;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Value("${batch.chunk-size}")
    private Integer chunkSize;

    @Bean
    public Job fileToKafkaJob(JobRepository jobRepository, Step fileToKafkaStep) {
        return new JobBuilder("fileToKafkaJob", jobRepository).start(fileToKafkaStep).build();
    }

    @Bean
    public Step fileToKafkaStep(JobRepository jobRepository, FlatFileItemReader<EodTransaction> flatFileItemReader,
            EodTransactionItemProcessor eodTransactionItemProcessor,
            ChunkMessageChannelItemWriter<EodTransactionEvent> chunkMessageChannelItemWriter,
            BatchJobListener batchJobListener) {
        return new StepBuilder("fileToKafkaStep", jobRepository).<EodTransaction, EodTransactionEvent>chunk(100)
                .reader(flatFileItemReader)
                .processor(eodTransactionItemProcessor)
                .writer(chunkMessageChannelItemWriter)
                .listener(batchJobListener)
                .faultTolerant()
                .skip(IncorrectTokenCountException.class)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EodTransaction> flatFileItemReader(
            @Value("#{jobParameters['input.filepath']}") String resource) {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(EodTransactionFieldNames.getFieldNames());
        lineTokenizer.setDelimiter(",");

        BeanWrapperFieldSetMapper<EodTransaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EodTransaction.class);

        Map<String, LineTokenizer> tokenizers = new HashMap<>();
        tokenizers.put("*", lineTokenizer);

        Map<String, FieldSetMapper<EodTransaction>> fieldSetMappers = new HashMap<>();
        fieldSetMappers.put("*", fieldSetMapper);

        PatternMatchingCompositeLineMapper<EodTransaction> lineMapper = new PatternMatchingCompositeLineMapper<>(
                tokenizers, fieldSetMappers);

        return new FlatFileItemReaderBuilder<EodTransaction>().name("flatFileItemReader")
                .resource(new FileSystemResource(resource))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public ChunkMessageChannelItemWriter<EodTransactionEvent> chunkMessageChannelItemWriter(
            MessagingTemplate messagingTemplate, QueueChannel chunkProcessorReplyChannel) {
        ChunkMessageChannelItemWriter<EodTransactionEvent> chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
        chunkMessageChannelItemWriter.setMessagingOperations(messagingTemplate);
        chunkMessageChannelItemWriter.setReplyChannel(chunkProcessorReplyChannel);

        return chunkMessageChannelItemWriter;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Batch configuration enabled.");
        logger.info("batch.chunk-size: {}", chunkSize);
    }
}
