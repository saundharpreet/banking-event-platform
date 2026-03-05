package com.harpreetsaund.transactionfileingestor.processor;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import com.harpreetsaund.transactionfileingestor.mapper.EodTransactionEventMapper;
import com.harpreetsaund.transactionfileingestor.model.EodTransaction;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EodTransactionItemProcessor implements ItemProcessor<EodTransaction, EodTransactionEvent> {

    private static final Logger logger = LoggerFactory.getLogger(EodTransactionItemProcessor.class);

    private final EodTransactionEventMapper eodTransactionEventMapper;

    public EodTransactionItemProcessor(EodTransactionEventMapper eodTransactionEventMapper) {
        this.eodTransactionEventMapper = eodTransactionEventMapper;
    }

    @Override
    public @Nullable EodTransactionEvent process(EodTransaction item) throws Exception {
        logger.debug("Processing item: {}", item);

        if (item != null && item.getTransactionId() != null) {
            return eodTransactionEventMapper.toEodTransactionEvent(item);
        }

        return null;
    }
}
