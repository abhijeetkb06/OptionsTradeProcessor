package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

public class BusinessTransactionData {
    private JsonObject dbDeal;
    private TradeData trades;
    private JsonObject position;
    private JsonObject openInterest;
    private JsonObject fwDbMsgDbSeq;
    private JsonObject multiAssetTransactionLog;
    private JsonObject highestMultiAssetTransactionLog;
    private JsonObject dbLatestTxnIdForTrades;
    private JsonObject fwDbMsgqOutSeq;

    public JsonObject getDbDeal() {
        return dbDeal;
    }

    public void setDbDeal(JsonObject dbDeal) {
        this.dbDeal = dbDeal;
    }

    public TradeData getTrades() {
        return trades;
    }

    public void setTrades(TradeData trades) {
        this.trades = trades;
    }

    public JsonObject getPosition() {
        return position;
    }

    public void setPosition(JsonObject position) {
        this.position = position;
    }

    public JsonObject getOpenInterest() {
        return openInterest;
    }

    public void setOpenInterest(JsonObject openInterest) {
        this.openInterest = openInterest;
    }

    public JsonObject getFwDbMsgDbSeq() {
        return fwDbMsgDbSeq;
    }

    public void setFwDbMsgDbSeq(JsonObject fwDbMsgDbSeq) {
        this.fwDbMsgDbSeq = fwDbMsgDbSeq;
    }

    public JsonObject getMultiAssetTransactionLog() {
        return multiAssetTransactionLog;
    }

    public void setMultiAssetTransactionLog(JsonObject multiAssetTransactionLog) {
        this.multiAssetTransactionLog = multiAssetTransactionLog;
    }

    public JsonObject getHighestMultiAssetTransactionLog() {
        return highestMultiAssetTransactionLog;
    }

    public void setHighestMultiAssetTransactionLog(JsonObject highestMultiAssetTransactionLog) {
        this.highestMultiAssetTransactionLog = highestMultiAssetTransactionLog;
    }

    public JsonObject getDbLatestTxnIdForTrades() {
        return dbLatestTxnIdForTrades;
    }

    public void setDbLatestTxnIdForTrades(JsonObject dbLatestTxnIdForTrades) {
        this.dbLatestTxnIdForTrades = dbLatestTxnIdForTrades;
    }

    public JsonObject getFwDbMsgqOutSeq() {
        return fwDbMsgqOutSeq;
    }

    public void setFwDbMsgqOutSeq(JsonObject fwDbMsgqOutSeq) {
        this.fwDbMsgqOutSeq = fwDbMsgqOutSeq;
    }
}