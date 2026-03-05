package com.harpreetsaund.transactionfileingestor.config;

import org.apache.commons.io.FileUtils;
import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.remote.session.SessionFactory;

import java.io.File;

@Configuration
@EnableBatchIntegration
public class BatchIntegrationConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(BatchIntegrationConfig.class);

    @Bean
    public DirectChannel batchJobOutputChannel() {
        return new DirectChannel();
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
    public IntegrationFlow batchJobCleanupFlow(SessionFactory<SftpClient.DirEntry> sessionFactory) {
        return IntegrationFlow.from("batchJobOutputChannel")
                .enrichHeaders(configurer -> configurer.headerExpression("remoteFilePath",
                        "headers['remoteFilePath'] + '/' + headers['file_remoteFile']"))
//                .handle(Sftp
//                        .outboundGateway(sessionFactory, AbstractRemoteFileOutboundGateway.Command.MV,
//                                "headers['remoteFilePath']")
//                        .renameExpression("headers['remoteFilePath'] + payload['status']"))
                .handle(message -> FileUtils.deleteQuietly(message.getHeaders().get("file_originalFile", File.class)))
                .get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Batch Integration configuration enabled.");
    }
}
