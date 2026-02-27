package com.harpreetsaund.transactionfileingestor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.jdbc.metadata.JdbcMetadataStore;
import org.springframework.integration.metadata.ConcurrentMetadataStore;

import javax.sql.DataSource;

@Configuration
public class MetadataStoreConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(MetadataStoreConfig.class);

    @Bean
    public ConcurrentMetadataStore metadataStore(DataSource dataSource) {
        JdbcMetadataStore metadataStore = new JdbcMetadataStore(dataSource);
        metadataStore.setCheckDatabaseOnStart(true);

        return metadataStore;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Metadata Store configuration enabled.");
    }
}
