package com.harpreetsaund.transactionfileingestor.processor;

import com.harpreetsaund.transaction.avro.EodTransactionEvent;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class EodTransactionEventChunkProcessor implements ChunkProcessor<EodTransactionEvent> {

    private static final Logger logger = LoggerFactory.getLogger(EodTransactionEventChunkProcessor.class);

    private final DirectChannel outboundKafkaEventChannel;

    public EodTransactionEventChunkProcessor(DirectChannel outboundKafkaEventChannel) {
        this.outboundKafkaEventChannel = outboundKafkaEventChannel;
    }

    @Override
    public void process(@NonNull Chunk<EodTransactionEvent> chunk, @NonNull StepContribution contribution)
            throws Exception {
        logger.debug("Processing chunk with {} items", chunk.getItems().size());

        chunk.getItems().forEach(eodTransactionEvent -> {
            logger.debug("Sending event to outbound channel: {}", eodTransactionEvent);
            outboundKafkaEventChannel.send(MessageBuilder.withPayload(eodTransactionEvent)
                    .setHeader(KafkaHeaders.KEY, eodTransactionEvent.getPayload().getTransactionId())
                    .build());
        });
    }
}
