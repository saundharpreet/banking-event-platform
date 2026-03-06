package com.harpreetsaund.transactionfileingestor.config;

import com.harpreetsaund.transactionfileingestor.service.MessageTransformService;
import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;

import java.io.File;

@Configuration
public class InboundChannelConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(InboundChannelConfig.class);

    @Value("${inbound-channel.remote-directory}")
    private String remoteDirectory;

    @Value("${inbound-channel.local-directory}")
    private String localDirectory;

    @Value("${inbound-channel.filename-pattern}")
    private String filenamePattern;

    @Value("${inbound-channel.poller.fixed-delay}")
    private Long pollerFixedDelay;

    @Value("${metadata-store.file-name-prefix}")
    private String metadataStoreFileNamePrefix;

    @Bean
    public DirectChannel fileToBatchJobChannel() {
        return new DirectChannel();
    }

    @Bean
    public ChainFileListFilter<SftpClient.DirEntry> fileListFilter(ConcurrentMetadataStore metadataStore) {
        ChainFileListFilter<SftpClient.DirEntry> filter = new ChainFileListFilter<>();
        filter.addFilter(new SftpRegexPatternFileListFilter(filenamePattern));
        filter.addFilter(new SftpPersistentAcceptOnceFileListFilter(metadataStore, metadataStoreFileNamePrefix));
        filter.addFilter(new AcceptOnceFileListFilter<>(1));

        return filter;
    }

    @Bean
    public IntegrationFlow inboundFileFlow(SessionFactory<SftpClient.DirEntry> sessionFactory,
            ChainFileListFilter<SftpClient.DirEntry> fileListFilter, MessageTransformService messageTransformService) {
        return IntegrationFlow
                .from(Sftp.inboundAdapter(sessionFactory)
                        .preserveTimestamp(true)
                        .maxFetchSize(1)
                        .remoteDirectory(remoteDirectory)
                        .autoCreateLocalDirectory(true)
                        .localDirectory(new File(localDirectory))
                        .filter(fileListFilter), spec -> spec.poller(p -> p.fixedDelay(pollerFixedDelay)))
                .transform(messageTransformService, "transformToBatchJobRequest")
                .channel("fileToBatchJobChannel")
                .get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Inbound Integration configuration enabled.");
        logger.info("inbound-channel.remote-directory: {}", remoteDirectory);
        logger.info("inbound-channel.local-directory: {}", localDirectory);
        logger.info("inbound-channel.filename-pattern: {}", filenamePattern);
        logger.info("inbound-channel.poller.fixed-delay: {}", pollerFixedDelay);
        logger.info("metadata-store.file-name-prefix: {}", metadataStoreFileNamePrefix);
    }
}
