package com.harpreetsaund.transactionfileingestor.config;

import liquibase.integration.spring.SpringLiquibase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class LiquibaseConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(LiquibaseConfig.class);

    @Value("${liquibase.change-log}")
    private String changeLog;

    @Bean
    public SpringLiquibase liquibase(DataSource dataSource) {
        SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setChangeLog(changeLog);
        liquibase.setDataSource(dataSource);

        return liquibase;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Liquibase configuration enabled.");
        logger.info("liquibase.change-log: {}", changeLog);
    }
}
