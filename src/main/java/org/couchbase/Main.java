package org.couchbase;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.kv.MutationResult;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.couchbase.client.java.Collection;


/**
 * This class demonstrates how to perform multiple key-value inserts and reads within a single transaction using Couchbase Java SDK.
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
        List<JsonObject> data = generateDBDealDataListOfDocuments();

        // Capture the start time
        long startTime = System.currentTimeMillis();

        // Multiple inserts test
        // List<MutationResult> result = simulateMultipleDbDeals(data);

        // Simulate transactions for DbDeal
        JsonObject dbDeal = generateDBDealJSONData();
        collection.upsert(dbDeal.getString("key"),dbDeal);

        // Simulate reads for DbDeal
        JsonObject resultDbDeal = collection.get(dbDeal.getString("key")).contentAsObject();
        System.out.println("Read DbDeal with Key: " + resultDbDeal.getString("key"));



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

        JsonObject jsonData = JsonObject.create();

        jsonData.put("action", "CREATE");
        jsonData.put("collection", "DbDeal");

        JsonObject entity = JsonObject.create();
        entity.put("sellTradeId", "49151749");
        entity.put("dealId", "24325637");

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
//        jsonData.put("key", "24325637");
        jsonData.put("key", UUID.randomUUID().toString());
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
    private static JsonObject generateTradeJSONData() {
        Random random = new Random();
        JsonObject trade = JsonObject.create();

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
        trade.put("sequenceNumber", Integer.toString(random.nextInt(100000)));
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
        immutableTradeAttributes.put("instrumentKey", createInstrumentKey(random));
        immutableTradeAttributes.put("dealId", Integer.toString(random.nextInt(100000)));
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
        trade.put("tradeId", Integer.toString(random.nextInt(100000)));
        trade.put("status", "ACTIVE");
        trade.put("key", UUID.randomUUID().toString());

        return trade;
    }

    private static JsonObject createFilterKeys(Random random) {
        JsonObject filterKeys = JsonObject.create();
        filterKeys.put("nonClearedStructureKey", "0");
        filterKeys.put("productStructureKey", "144396836679974912");
        return filterKeys;
    }

    private static JsonObject createInstrumentKey(Random random) {
        JsonObject instrumentKey = JsonObject.create();
        instrumentKey.put("id", Integer.toString(random.nextInt(100000)));
        return instrumentKey;
    }
}

