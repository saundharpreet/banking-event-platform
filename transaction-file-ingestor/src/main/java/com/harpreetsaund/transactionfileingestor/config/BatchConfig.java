package com.harpreetsaund.transactionfileingestor.config;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transactionfileingestor.listener.BatchJobListener;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import com.harpreetsaund.transactionfileingestor.model.constants.EodTransactionFieldNames;
import com.harpreetsaund.transactionfileingestor.processor.EodTransactionItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
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
import org.springframework.batch.infrastructure.item.kafka.KafkaItemWriter;
import org.springframework.batch.infrastructure.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Isolation;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@EnableJdbcJobRepository(isolationLevelForCreate = Isolation.REPEATABLE_READ)
public class BatchConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Value("${batch.chunk-size}")
    private Integer chunkSize;

    @Bean
    public Job fileToKafkaJob(JobRepository jobRepository, Step fileToKafkaStep) {
        return new JobBuilder("fileToKafkaJob", jobRepository).start(fileToKafkaStep).build();
    }

    @Bean
    public Step fileToKafkaStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
            FlatFileItemReader<EodTransaction> flatFileItemReader,
            EodTransactionItemProcessor eodTransactionItemProcessor,
            KafkaItemWriter<String, EodTransactionEvent> kafkaItemWriter, BatchJobListener batchJobListener) {
        return new StepBuilder("fileToKafkaStep", jobRepository).<EodTransaction, EodTransactionEvent>chunk(100)
                .transactionManager(platformTransactionManager)
                .reader(flatFileItemReader)
                .processor(eodTransactionItemProcessor)
                .writer(kafkaItemWriter)
                .listener(batchJobListener)
                .faultTolerant()
                .skip(IncorrectTokenCountException.class)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EodTransaction> flatFileItemReader(
            @Value("#{jobParameters['input.filepath']}") String filepath) {
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
                .strict(false)
                .targetType(EodTransaction.class)
                .resource(new FileSystemResource(filepath))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public KafkaItemWriter<String, EodTransactionEvent> kafkaItemWriter(
            KafkaTemplate<String, EodTransactionEvent> kafkaTemplate) {
        return new KafkaItemWriterBuilder<String, EodTransactionEvent>().kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(event -> event.getPayload().getTransactionId())
                .build();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Batch configuration enabled.");
        logger.info("batch.chunk-size: {}", chunkSize);
    }
}
