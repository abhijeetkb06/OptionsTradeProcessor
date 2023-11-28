package org.couchbase;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import reactor.core.publisher.Flux;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;


/**
 * This class demonstrates how to perform multiple key-value inserts and reads within a single transaction using Couchbase Java SDK.
 */
public class Main {

    private static final String CONNECTION_STRING = "couchbases://cb.bgzm40pdb7nphef.cloud.couchbase.com";
    private static final String USERNAME = "abhijeet";
    private static final String PASSWORD = "Password@P1";
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
        List<JsonObject> data = generateData();

        // Capture the start time
        long startTime = System.currentTimeMillis();

        // TODO: Simulate the transaction process
//        simulateTransaction(collection);

        List<MutationResult> result = bulkInsert(data);

        // Capture the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the elapsed time
        long elapsedTime = endTime - startTime;
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");

        // Close the connection to the Couchbase cluster
        cluster.disconnect();
    }

    private static void simulateTransaction(Collection collection) {
        // Iterate through the provided JSON data and perform corresponding Couchbase operations
        simulateDbDeal(collection);
        simulateTrades(collection);
        simulatePositions(collection);
        // Add other collections as needed

        // Commit or rollback the transaction based on your business logic
        // For simplicity, we'll assume the transaction is always successful
        System.out.println("Transaction committed successfully.");
    }

    private static void simulateDbDeal(Collection collection) {
        // Sample data for DbDeal
        JsonObject dbDealData = JsonObject.fromJson("{" +
                "\"action\":\"CREATE\"," +
                "\"collection\":\"DbDeal\"," +
                "\"entity\":{...}," +
                "\"key\":\"24325637\"" +
                "}");

        // Perform the Couchbase operation for DbDeal
        collection.upsert("DbDeal::24325637", dbDealData);
    }

    private static void simulateTrades(Collection collection) {
        // Sample data for TRADES
        JsonObject tradesData = JsonObject.fromJson("{" +
                "\"action\":\"CREATE\"," +
                "\"collection\":\"TRADES\"," +
                "\"entity\":{...}," +
                "\"key\":\"49151493\"" +
                "}");

        // Perform the Couchbase operation for TRADES
        collection.upsert("TRADES::49151493", tradesData);
        // Repeat for other TRADES data as needed
    }

    private static void simulatePositions(Collection collection) {
        // Sample data for POSITION
        JsonObject positionData = JsonObject.fromJson("{" +
                "\"action\":\"CREATE\"," +
                "\"collection\":\"POSITIONS\"," +
                "\"entity\":{...}," +
                "\"key\":\"POSITIONS::1460054::71\"" +
                "}");

        // Perform the Couchbase operation for POSITION
        collection.upsert("POSITIONS::1460054::71", positionData);
        // Repeat for other POSITION data as needed
    }

    // Add similar methods for other collections as needed

    private static List<MutationResult> bulkInsert(List<JsonObject> data) {

        return Flux.fromIterable(data)
                .flatMap(doc -> collection.reactive().upsert(doc.getString("MessageId"),doc))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }


    public static List<JsonObject> generateData() {

        return Flux.range(0, 30)
                .map(i -> generateMockData(i))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }

    private static JsonObject generateMockData(int index) {
        Random random = new Random();

        JsonObject jsonData = JsonObject.from(new LinkedHashMap<>());

        jsonData.put("MessageId", UUID.randomUUID().toString());
        jsonData.put("DeviceId", "vehicle" + random.nextInt(100));
        jsonData.put("EventTime", Instant.now().toString());

        // Orgs represented as an array of objects
        JsonArray orgsArray = JsonArray.from("org1" + random.nextInt(1000), "org2" + random.nextInt(1000));
        jsonData.put("Orgs", orgsArray);

        JsonObject payload = JsonObject.from(new LinkedHashMap<>());

        JsonObject telematics = JsonObject.from(new LinkedHashMap<>());
        telematics.put("vehicleId", "ABC" + random.nextInt(1000));

        JsonObject location = JsonObject.from(new LinkedHashMap<>());
        location.put("latitude", 30 + random.nextDouble() * 20);
        location.put("longitude", -120 + random.nextDouble() * 60);
        telematics.put("location", location);

        telematics.put("speed", random.nextInt(100));
        telematics.put("fuelLevel", random.nextInt(100));
        telematics.put("engineStatus", random.nextBoolean() ? "running" : "stopped");

        JsonObject tirePressure = JsonObject.from(new LinkedHashMap<>());
        tirePressure.put("frontLeft", 28 + random.nextInt(10));
        tirePressure.put("frontRight", 28 + random.nextInt(10));
        tirePressure.put("rearLeft", 28 + random.nextInt(10));
        tirePressure.put("rearRight", 28 + random.nextInt(10));
        telematics.put("tirePressure", tirePressure);

        JsonObject driver = JsonObject.from(new LinkedHashMap<>());
        driver.put("name", "Driver" + random.nextInt(1000));
        driver.put("licenseNumber", "ABC" + random.nextInt(1000));
        driver.put("status", random.nextBoolean() ? "active" : "inactive");
        telematics.put("driver", driver);
        payload.put("telematics", telematics);
        jsonData.put("Payload", payload);
        return jsonData;
    }
}

