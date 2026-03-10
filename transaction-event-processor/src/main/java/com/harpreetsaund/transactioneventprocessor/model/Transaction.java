package com.harpreetsaund.transactioneventprocessor.model;

import jakarta.persistence.*;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "transactions", indexes = { @Index(name = "idx_transactions_account", columnList = "accountNumber"),
        @Index(name = "idx_transactions_txn_time", columnList = "transactionTimestamp"),
        @Index(name = "idx_transactions_transaction_id", columnList = "transactionId") }, uniqueConstraints = {
                @UniqueConstraint(name = "uk_event_id", columnNames = "eventId") })
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String eventId;

    @Column(nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private String accountNumber;

    @Column(nullable = false)
    private String transactionType;

    @Column(nullable = false, precision = 18, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    private String merchantName;

    private String channel;

    @Column(nullable = false)
    private Instant transactionTimestamp;

    @Column(nullable = false)
    private String sourceSystem;

    private String topicName;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Instant getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void setTransactionTimestamp(Instant transactionTimestamp) {
        this.transactionTimestamp = transactionTimestamp;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", id).append("eventId", eventId)
                .append("transactionId", transactionId).append("accountNumber", accountNumber)
                .append("transactionType", transactionType).append("amount", amount).append("currency", currency)
                .append("merchantName", merchantName).append("channel", channel)
                .append("transactionTimestamp", transactionTimestamp).append("sourceSystem", sourceSystem)
                .append("topicName", topicName).append("createdAt", createdAt).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Transaction that = (Transaction) o;

        return new EqualsBuilder().append(id, that.id).append(eventId, that.eventId)
                .append(transactionId, that.transactionId).append(accountNumber, that.accountNumber)
                .append(transactionType, that.transactionType).append(amount, that.amount)
                .append(currency, that.currency).append(merchantName, that.merchantName).append(channel, that.channel)
                .append(transactionTimestamp, that.transactionTimestamp).append(sourceSystem, that.sourceSystem)
                .append(topicName, that.topicName).append(createdAt, that.createdAt).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(id).append(eventId).append(transactionId).append(accountNumber)
                .append(transactionType).append(amount).append(currency).append(merchantName).append(channel)
                .append(transactionTimestamp).append(sourceSystem).append(topicName).append(createdAt).toHashCode();
    }
}
