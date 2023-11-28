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

    private static final String CONNECTION_STRING = "couchbase://ec2-3-143-153-43.us-east-2.compute.amazonaws.com";
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

        List<MutationResult> result = simulateDbDeal(data);

        // Capture the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the elapsed time
        long elapsedTime = endTime - startTime;
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");

        // Close the connection to the Couchbase cluster
        cluster.disconnect();
    }

    private static List<MutationResult> simulateDbDeal(List<JsonObject> data) {

        return Flux.fromIterable(data)
                .flatMap(doc -> collection.reactive().upsert(doc.getString("key"),doc))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }

    public static List<JsonObject> generateDBDealDataListOfDocuments() {

        return Flux.range(0, 30)
                .map(i -> generateDBDealJSONData(i))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }

    private static JsonObject generateDBDealJSONData(int index) {
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
}

