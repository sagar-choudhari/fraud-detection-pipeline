package com.frauddetection.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.UUID;

public class Transaction {

    private String txnId;
    private String userId;
    private double amount;
    private String merchant;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;

    public Transaction(){}

    public static Transaction createRandom(){
        Transaction t = new Transaction();
        t.txnId = UUID.randomUUID().toString();
        t.userId = "user_"+(int)(Math.random()*(100));
        t.amount = Math.round(Math.random() * 10000.0 * 100.0) / 100.0;
        t.merchant = randomMerchant();
        t.timestamp = LocalDateTime.now();
        return t;
    }

    private static String randomMerchant() {
        String[] merchants = {"Amazon", "Flipkart", "Swiggy", "Zomato", "HDFC_ATM", "ICICI_ATM", "Uber", "Ola"};
        return merchants[(int)(Math.random() * merchants.length)];
    }

    public String getTxnId()           { return txnId; }
    public String getUserId()          { return userId; }
    public double getAmount()          { return amount; }
    public String getMerchant()        { return merchant; }
    public LocalDateTime getTimestamp(){ return timestamp; }

    public String toString(){
        return "Transaction{txnId='" + txnId + "', userId='" + userId +
                "', amount=" + amount + ", merchant='" + merchant +
                "', timestamp=" + timestamp + "}";
    }

}
