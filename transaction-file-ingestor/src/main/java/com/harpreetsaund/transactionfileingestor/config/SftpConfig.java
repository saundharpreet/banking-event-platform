package com.harpreetsaund.transactionfileingestor.config;

import io.micrometer.common.util.StringUtils;
import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;

@Configuration
public class SftpConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(SftpConfig.class);

    @Value("${sftp.host}")
    private String host;

    @Value("${sftp.port}")
    private Integer port;

    @Value("${sftp.username}")
    private String username;

    @Value("${sftp.password}")
    private String password;

    @Bean
    public SessionFactory<SftpClient.DirEntry> sessionFactory() {
        DefaultSftpSessionFactory sessionFactory = new DefaultSftpSessionFactory();
        sessionFactory.setHost(host);
        sessionFactory.setPort(port);
        sessionFactory.setUser(username);
        sessionFactory.setPassword(password);
        sessionFactory.setAllowUnknownKeys(true);

        return new CachingSessionFactory<>(sessionFactory);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("SFTP configuration enabled.");
        logger.info("sftp.host: {}", host);
        logger.info("sftp.port: {}", port);
        logger.info("sftp.username: {}", username);
        logger.info("sftp.password: {}", StringUtils.isBlank(password) ? "<blank>" : "<hidden>");
    }
}
