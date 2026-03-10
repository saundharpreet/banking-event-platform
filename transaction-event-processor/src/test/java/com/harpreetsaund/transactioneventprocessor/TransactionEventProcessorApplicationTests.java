package com.harpreetsaund.transactioneventprocessor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class TransactionEventProcessorApplicationTests {

    @Test
    void contextLoads() {
        String contextLoadsMessage = "Application context loaded successfully.";
        assertNotNull(contextLoadsMessage);
    }
}
