package org.couchbase;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.kv.MutationResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import com.couchbase.client.java.Collection;


/**
 * This class demonstrates how to perform multiple key-value inserts and reads using Couchbase Java SDK.
 */
public class Main {
//Capella Connection
   /* private static final String CONNECTION_STRING = "couchbases://cb.bgzm40pdb7nphef.cloud.couchbase.com";
    private static final String USERNAME = "abhijeet";
    private static final String PASSWORD = "Password@P1";*/

//    private static final String CONNECTION_STRING = "couchbase://ec2-3-143-153-43.us-east-2.compute.amazonaws.com";

    // Local
    private static final String CONNECTION_STRING = "couchbase://localhost";
    private static final String USERNAME = "Administrator";
    private static final String PASSWORD = "password";
    private static final String BUCKET = "occ";
    private static final String SCOPE = "_default";
    private static final String COLLECTION = "_default";
    private static final Cluster cluster;
    private static final Bucket bucket;
    private static final Scope scope;
    private static final Collection collection;

    static {
        cluster = Cluster.connect(
                CONNECTION_STRING,
                ClusterOptions.clusterOptions(USERNAME, PASSWORD).environment(env -> {
                    env.applyProfile("wan-development");
                })
        );

        bucket = cluster.bucket(BUCKET);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        scope = bucket.scope(SCOPE);
        collection = scope.collection(COLLECTION);
    }

    public static void main(String[] args) {

 /*       // Multiple inserts test using reactive APIs
        long startTime = System.currentTimeMillis();
        List<JsonObject> data = generateDBDealDataListOfDocuments();
        List<MutationResult> result = simulateMultipleDbDeals(data);*/

        // Capture the start time
        long startTime = System.currentTimeMillis();

        // Simulate writes for DbDeal
        JsonObject dbDeal = generateDBDealJSONData();
        String dbDealKey = dbDeal.getString("key");
        collection.upsert(dbDealKey,dbDeal);

        // Simulate reads for DbDeal
        collection.get(dbDealKey);
//      System.out.println("Read DbDeal with Key: " + resultDbDeal.getString("key"));

        // Simulate writes for Trades
        TradeData trades = generateTradeJSONData(dbDealKey);
        collection.upsert(trades.getTradeId(),trades.getTrade());

        // Simulate reads for Trades
        collection.get(trades.getTradeId());

        // Simulate writes for Positions
        JsonObject position = generatePositionJSONData(trades);
        collection.upsert(trades.getInstrumentKey(),position);

        // Simulate reads for Positions
        collection.get(trades.getInstrumentKey());

        // Simulate writes for OpenInterest
        JsonObject openInterest = generateOpenInterestJSONObject(trades);
        collection.upsert(trades.getInstrumentKey(),openInterest);

        // Simulate reads for openInterest
        collection.get(trades.getInstrumentKey());

        // Simulate writes for fwDbMsgDbSeq
        JsonObject fwDbMsgDbSeq =  generateFwDbXSeqJsonObject();
        collection.upsert(fwDbMsgDbSeq.getString("key"),fwDbMsgDbSeq);

        // Simulate reads for fwDbMsgDbSeq
        collection.get(fwDbMsgDbSeq.getString("key"));

        // Simulate writes for multiAssetTransactionLog
        JsonObject multiAssetTransactionLog =  generateMultiAssetTransactionLogJsonObject();
        collection.upsert(multiAssetTransactionLog.getString("key"),multiAssetTransactionLog);

        // Simulate reads for multiAssetTransactionLog
        collection.get(multiAssetTransactionLog.getString("key"));

        // Simulate writes for highestMultiAssetTransactionLog
        JsonObject highestMultiAssetTransactionLog =  generateHighestMultiAssetTransactionLogJsonObject();
        collection.upsert(highestMultiAssetTransactionLog.getString("key"),highestMultiAssetTransactionLog);

        // Simulate reads for highestMultiAssetTransactionLog
        collection.get(highestMultiAssetTransactionLog.getString("key"));

        // Simulate writes for DbLatestTxnIdForTrades
        JsonObject dbLatestTxnIdForTrades =  generateDbLatestTxnIdForTradesJsonObject();
        collection.upsert(dbLatestTxnIdForTrades.getString("key"),dbLatestTxnIdForTrades);

        // Simulate reads for DbLatestTxnIdForTrades
        collection.get(dbLatestTxnIdForTrades.getString("key"));

        // Simulate writes for FwDbMsgqOutSeq
        JsonObject fwDbMsgqOutSeq = generateFwDbMsgqOutSeqJsonObject();
        collection.upsert(fwDbMsgqOutSeq.getString("key"),fwDbMsgqOutSeq);

        // Simulate reads for FwDbMsgqOutSeq
        collection.get(fwDbMsgqOutSeq.getString("key"));

        // Capture the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the elapsed time
        long elapsedTime = endTime - startTime;
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");

        // Close the connection to the Couchbase cluster
        cluster.disconnect();
    }

    private static List<MutationResult> simulateMultipleDbDeals(List<JsonObject> data) {

        return Flux.fromIterable(data)
                .flatMap(doc -> collection.reactive().upsert(doc.getString("key"),doc))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();

    }

    public static List<JsonObject> generateDBDealDataListOfDocuments() {

        return Flux.range(0, 30)
                .map(i -> generateDBDealJSONData())
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }

    private static JsonObject generateDBDealJSONData() {
        Random random = new Random();
        String dbDealId = Integer.toString(random.nextInt(100000));
        JsonObject jsonData = JsonObject.create();

        jsonData.put("action", "CREATE");
        jsonData.put("collection", "DbDeal");

        JsonObject entity = JsonObject.create();
        entity.put("sellTradeId", "49151749");
        entity.put("dealId", dbDealId);

        // Mocking "buy" section
        JsonObject buy = JsonObject.create();
        buy.put("allocation", createAllocationData(random));
        entity.put("buy", buy);

        entity.put("transactionTime", Instant.now().toString());
        entity.put("entryTxnId", "24325637");
        entity.put("isBackedOutBusted", false);
        entity.put("buyTradeId", "49151493");
        entity.put("price", "99");

        // Mocking "instrumentSpecificDealData" section
        JsonObject instrumentSpecificDealData = JsonObject.create();
        instrumentSpecificDealData.put("$type", "ConfirmationAgreement");
        entity.put("instrumentSpecificDealData", instrumentSpecificDealData);

        // Mocking "tradeSequenceNumbers" section
        JsonArray tradeSequenceNumbers = JsonArray.from("48651269", "48651525");
        entity.put("tradeSequenceNumbers", tradeSequenceNumbers);

        entity.put("tradeBusinessDate", "2018-02-01");
        entity.put("timestamp", Instant.now().toString());
        entity.put("quantity", "50");

        // Mocking "filterKeys" section
        JsonObject filterKeys = JsonObject.create();
        filterKeys.put("nonClearedStructureKey", "0");
        filterKeys.put("productStructureKey", "144396836679974912");
        entity.put("filterKeys", filterKeys);

        // Mocking "instrumentKey" section
        JsonObject instrumentKey = JsonObject.create().put("id", "1460054");
        entity.put("instrumentKey", instrumentKey);

        // Mocking "sell" section
        JsonObject sell = JsonObject.create();
        sell.put("allocation", createAllocationData(random));
        entity.put("sell", sell);

        entity.put("enteringUser", "RTC_SUPER");
        entity.put("tradeCurrency", "USD");
        entity.put("clearingSequenceNumber", "84618645841335");
        entity.put("$type", "DbDeal");
        entity.put("wasRejected", false);
        entity.put("clientDealId", "1756384618645841335");
        entity.put("asOfDate", "2023-11-20");
        entity.put("typeOfTrade", "REGULAR");
        entity.put("dealSource", "6");
        entity.put("status", "ACTIVE");

        jsonData.put("entity", entity);
        jsonData.put("key", dbDealId);
        return jsonData;
    }

    private static JsonObject createAllocationData(Random random) {
        JsonObject allocationData = JsonObject.create();

        allocationData.put("quantity", "50");
        allocationData.put("cmtaFlag", true);
        allocationData.put("openOrClose", "OPEN");
        allocationData.put("cmtaName", String.valueOf(random.nextInt(10000)));

        JsonArray defaultReasons = JsonArray.from("MARKET_MAKER_NOT_FOUND", "DEFAULT_AT_TAKEUP");
        allocationData.put("defaultReasons", defaultReasons);

        allocationData.put("originalCmNumber", String.valueOf(random.nextInt(10000)));
        allocationData.put("accountId", String.valueOf(random.nextInt(1000)));
        allocationData.put("cmtaResult", "DEFAULTED_BACK_TO_EXECUTOR");
        allocationData.put("originalAccountType", "M");
        allocationData.put("isEnteredOnDefaultAccount", true);
        allocationData.put("allocationFlag", false);
        allocationData.put("destinationCmn", String.valueOf(random.nextInt(10000)));
        allocationData.put("destinationAccount", String.valueOf(random.nextInt(1000)));

        return allocationData;
    }

    // Trades object
    private static TradeData generateTradeJSONData(String dbDealKey) {
        Random random = new Random();
        JsonObject trade = JsonObject.create();
        String sequenceNumber = Integer.toString(random.nextInt(100000));
        String tradeId = Integer.toString(random.nextInt(100000));
        String instrumentKey = Integer.toString(random.nextInt(100000));

        trade.put("remainingQuantity", Integer.toString(random.nextInt(100)));
        trade.put("reason", "TRADE");
        trade.put("reservedQuantity", "0");
        trade.put("originalQuantity", Integer.toString(random.nextInt(100)));
        trade.put("isExchangeTrade", true);
        trade.put("transactionTime", "1970-01-20 16:22:24.827");
        trade.put("hasWarnings", false);
        trade.put("isBackedOutBusted", false);
        trade.put("cmtaResult", "SUCCESSFULLY_BOOKED_AT_TAKEUP");
        trade.put("originalAccountType", "M");
        trade.put("price", Integer.toString(random.nextInt(100)));
        trade.put("nextTradeIds", JsonArray.create());
        trade.put("isEnteredOnDefaultAccount", true);

        // Mocking "instrumentSpecificDealData" section
        JsonObject instrumentSpecificDealData = JsonObject.create();
        instrumentSpecificDealData.put("$type", "ConfirmationAgreement");
        trade.put("instrumentSpecificDealData", instrumentSpecificDealData);

        trade.put("cmtaSuccessful", false);
        trade.put("destinationCm", Integer.toString(random.nextInt(10000)));
        trade.put("initialValue", "0");
        trade.put("premiumPayment", "495000.00");
        trade.put("tradeBusinessDate", "2018-02-01");
        trade.put("timestamp", Instant.now().toString());
        trade.put("previouslyReported", false);
        trade.put("sequenceNumber", sequenceNumber);
        trade.put("cmtaExecutor", Integer.toString(random.nextInt(10000)));
        trade.put("moveTradeIds", JsonArray.create());
        trade.put("cmtaFlag", true);
        trade.put("tradeScenario", "CMTA_VALID");
        trade.put("openOrClose", "OPEN");
        trade.put("previousTradeIds", JsonArray.create());

        // Mocking "immutableTradeAttributes" section
        JsonObject immutableTradeAttributes = JsonObject.create();
        immutableTradeAttributes.put("isBuy", random.nextBoolean());
        immutableTradeAttributes.put("filterKeys", createFilterKeys(random));
        immutableTradeAttributes.put("instrumentKey", instrumentKey);
        immutableTradeAttributes.put("dealId", dbDealKey);
        immutableTradeAttributes.put("tradeSourceId", "6");
        immutableTradeAttributes.put("clientDealId", Long.toString(random.nextLong()));
        immutableTradeAttributes.put("user", "RTC_SUPER");
        trade.put("immutableTradeAttributes", immutableTradeAttributes);

        trade.put("isApgTrade", false);
        trade.put("originalDestinationCm", Integer.toString(random.nextInt(10000)));
        trade.put("tradeCurrency", "USD");
        trade.put("clearingSequenceNumber", "84618645841335");
        trade.put("$type", "DbTrade");
        trade.put("originalCmNumber", Integer.toString(random.nextInt(10000)));
        trade.put("accountId", Integer.toString(random.nextInt(1000)));
        trade.put("wasRejected", false);
        trade.put("marketValueEligible", false);
        trade.put("cmtaExecutorId", Integer.toString(random.nextInt(10000)));
        trade.put("isGrouped", false);
        trade.put("inputSource", "FIXML_REALTIME");
        trade.put("allocationFlag", false);
        trade.put("asOfDate", "2023-11-20");
        trade.put("typeOfTrade", "REGULAR");
        trade.put("destinationAccount", Integer.toString(random.nextInt(1000)));
        trade.put("tradeId", tradeId);
        trade.put("status", "ACTIVE");
        trade.put("key", tradeId);

        return new TradeData(sequenceNumber, tradeId, trade, instrumentKey);
    }

    private static JsonObject createFilterKeys(Random random) {
        JsonObject filterKeys = JsonObject.create();
        filterKeys.put("nonClearedStructureKey", "0");
        filterKeys.put("productStructureKey", "144396836679974912");
        return filterKeys;
    }

    private static JsonObject generatePositionJSONData(TradeData tradeData) {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();

        jsonObject.put("action", "CREATE");
        jsonObject.put("collection", "POSITIONS");

        JsonObject entity = JsonObject.create();
        entity.put("reason", "TRADE");
        entity.put("accountFilterKey", JsonObject.create().put("accountStructureKey", "72104873037987841"));
        entity.put("t1Premium", "-495000.00");
        entity.put("t2Premium", "0");
        entity.put("initializeDate", 0);
        entity.put("shortExcessQuantity", "0");

        JsonObject instrumentSpecificPosition = JsonObject.create().put("$type", "InstrumentSpecificPosition");
        JsonObject position = JsonObject.create()
                .put("instrumentSpecificPosition", instrumentSpecificPosition)
                .put("quantity", "0")
                .put("marketValue", "0")
                .put("initialValue", "0");

        entity.put("longStartOfDayPosition", position);
        entity.put("previousLongStartOfDayPosition", position);
        entity.put("shortPosition", position);
        entity.put("positionBeforeCorporateActionUpdate", JsonArray.create());
        entity.put("hasEthPosition", false);

        JsonObject longPosition = JsonObject.create()
                .put("instrumentSpecificPosition", instrumentSpecificPosition)
                .put("quantity", "50")
                .put("marketValue", "0")
                .put("initialValue", "0");
        entity.put("longPosition", longPosition);
        entity.put("previousShortStartOfDayPosition", position);

        JsonObject filterKeys = JsonObject.create()
                .put("nonClearedStructureKey", "0")
                .put("productStructureKey", "144396836679974912");
        entity.put("filterKeys", filterKeys);

        entity.put("posKeep", "GROSS");
        entity.put("sodShortMarketValueDelta", "0");
        entity.put("standardIntentionQuantity", "0");

        JsonObject buckets = JsonObject.create()
                .put("assignments", position)
                .put("sellOpen", position)
                .put("exercises", position)
                .put("sellClose", position)
                .put("buyOpen", longPosition)
                .put("buyClose", position)
                .put("openUpCloseOutTransactions", JsonArray.create());
        entity.put("buckets", buckets);

        entity.put("shortStartOfDayPosition", position);
        entity.put("$type", "DbPosition");
        entity.put("finalizeDate", 0);
        entity.put("mergeDate", 0);
        entity.put("sodLongMarketValueDelta", "0");

        JsonObject positionKey = JsonObject.create()
                .put("date", -1)
                .put("subId", "-1")
                .put("accountId", "71")
                .put("instrumentKey", tradeData.getInstrumentKey());
        entity.put("positionKey", positionKey);

        entity.put("longExcessQuantity", "0");
        entity.put("tradeId", tradeData.getTradeId());

        jsonObject.put("entity", entity);

        JsonObject key = JsonObject.create()
                .put("date", -1)
                .put("subId", "-1")
                .put("accountId", "71")
                .put("instrumentKey", tradeData.getInstrumentKey());
        jsonObject.put("key", key);

        return jsonObject;
    }

    private static JsonObject generateOpenInterestJSONObject(TradeData tradeData) {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();

        jsonObject.put("action", "UPDATE");
        jsonObject.put("collection", "DbOpenInterest");
        jsonObject.put("previousTransactionId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("sequenceNumber", Integer.toString(random.nextInt(10000)));
        jsonObject.put("productKeyId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("openInterest", Integer.toString(random.nextInt(10000)));
        jsonObject.put("tradableInstrument", tradeData.getInstrumentKey());
        jsonObject.put("ethOpenInterest", "0");
        jsonObject.put("$type", "DbOpenInterest");
        jsonObject.put("key", tradeData.getInstrumentKey());

        return jsonObject;
    }

    private static JsonObject generateFwDbXSeqJsonObject() {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();
        jsonObject.put("action", "UPDATE");
        jsonObject.put("collection", "FwDbXSeq");
        jsonObject.put("previousTransactionId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("journalId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("journalSequence", Integer.toString(random.nextInt(10000)));
        jsonObject.put("journalTimestamp", "1700544827855990801");
        jsonObject.put("$type", "XJournalSequence");
        jsonObject.put("key", Integer.toString(random.nextInt(10000)));

        return jsonObject;
    }

    private static JsonObject generateMultiAssetTransactionLogJsonObject() {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();

        jsonObject.put("action", "CREATE");
        jsonObject.put("collection", "MultiAssetTransactionLogs");
        jsonObject.put("$type", "DbMultiAssetTransactionLog");
        jsonObject.put("timestamp", "2023-11-21 05:33:47.855");
        jsonObject.put("txnId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("key", Integer.toString(random.nextInt(10000)));

        return jsonObject;
    }

    private static JsonObject generateHighestMultiAssetTransactionLogJsonObject() {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();
        jsonObject.put("action", "UPDATE");
        jsonObject.put("collection", "HighestMultiAssetTransactionLogs");
        jsonObject.put("previousTransactionId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("key", "com.cinnober.position.event.protocol.position.ActualPositionEvent");
        jsonObject.put("$type", "DbMultiAssetHighestTransactionLog");
        jsonObject.put("txnId", "24325637");

        return jsonObject;
    }

    private static JsonObject generateDbLatestTxnIdForTradesJsonObject() {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();
        jsonObject.put("action", "UPDATE");
        jsonObject.put("collection", "DbLatestTxnIdForTrades");
        jsonObject.put("previousTransactionId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("latestTxnId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("key", Integer.toString(random.nextInt(10000)));
        jsonObject.put("$type", "DbLatestTxnIdForTrades");

        return jsonObject;
    }

    private static JsonObject generateFwDbMsgqOutSeqJsonObject() {
        JsonObject jsonObject = JsonObject.create();
        Random random = new Random();

        jsonObject.put("action", "UPDATE");
        jsonObject.put("collection", "FwDbMsgqOutSeq");
        jsonObject.put("previousTransactionId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("queueId", Integer.toString(random.nextInt(10000)));
        jsonObject.put("associatedTxSequence", Integer.toString(random.nextInt(10000)));
        jsonObject.put("queueSequence", Integer.toString(random.nextInt(10000)));
        jsonObject.put("$type", "FwDbQueueOutSequence");
        jsonObject.put("key", Integer.toString(random.nextInt(10000)));

        return jsonObject;
    }
}
 class TradeData {
    private String sequenceNumber;
    private String tradeId;
    private JsonObject trade;
    private String instrumentKey;

    public TradeData(String sequenceNumber, String tradeId, JsonObject trade, String instrumentKey) {
        this.sequenceNumber = sequenceNumber;
        this.tradeId = tradeId;
        this.trade = trade;
        this.instrumentKey = instrumentKey;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public String getTradeId() {
        return tradeId;
    }

    public JsonObject getTrade() {
        return trade;
    }

    public String getInstrumentKey() {
        return instrumentKey;
    }
}
