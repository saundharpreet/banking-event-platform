package com.harpreetsaund.transactioneventprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
public class TransactionConfig implements InitializingBean {

    public static final Logger logger = LoggerFactory.getLogger(TransactionConfig.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Transaction configuration enabled.");
    }
}
