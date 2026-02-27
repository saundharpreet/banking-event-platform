package com.harpreetsaund.transactionfileingestor.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatasourceConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DatasourceConfig.class);

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Datasource configuration enabled.");
        logger.info("spring.datasource.url: {}", url);
        logger.info("spring.datasource.username: {}", username);
        logger.info("spring.datasource.password: {}", StringUtils.isBlank(password) ? "<blank>" : "<hidden>");
        logger.info("spring.datasource.driver-class-name: {}", driverClassName);
    }
}
