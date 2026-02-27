package com.harpreetsaund.transactionfileingestor.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class EodTransaction {

    private String transactionId;
    private String accountNumber;
    private String type;
    private String amount;
    private String currency;
    private String timestamp;
    private String merchant;
    private String channel;

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("transactionId", transactionId).append("accountNumber", accountNumber)
                .append("type", type).append("amount", amount).append("currency", currency)
                .append("timestamp", timestamp).append("merchant", merchant).append("channel", channel).toString();
    }
}
