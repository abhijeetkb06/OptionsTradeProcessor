package org.couchbase;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import reactor.core.publisher.Flux;
import com.couchbase.client.java.kv.MutationResult;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Collection;
import reactor.core.publisher.Mono;


/**
 * This class simulates business transaction using multiple key-value inserts and reads using Couchbase Java SDK.
 */
public class Main {

    private static final Collection collection = CouchbaseConfig.getCollection();
    public static void main(String[] args) {

       /* // Multiple inserts test using reactive APIs
        long startTime = System.currentTimeMillis();
        List<JsonObject> data = generateDBDealDataListOfDocuments();
        List<MutationResult> result = simulateMultipleDbDeals(data);*/

        BusinessTransactionData businessTransaction = generateMockDataForBusinessTransaction();

        // Capture the start time
        long startTime = System.currentTimeMillis();
        simulateBusinessTransaction(businessTransaction);

        // Capture the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the elapsed time
        long elapsedTime = endTime - startTime;
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");

        // Close the connection to the Couchbase cluster
        CouchbaseConfig.getCluster().disconnect();
    }

    private static BusinessTransactionData generateMockDataForBusinessTransaction() {
        BusinessTransactionData mockData = new BusinessTransactionData();
        mockData.setDbDeal(BusinessTransactionJsonGenerator.generateDBDealJSONData());
        mockData.setTrades(BusinessTransactionJsonGenerator.generateTradeJSONData(mockData.getDbDeal().getString("key")));
        mockData.setPosition(BusinessTransactionJsonGenerator.generatePositionJSONData(mockData.getTrades()));
        mockData.setOpenInterest(BusinessTransactionJsonGenerator.generateOpenInterestJSONObject(mockData.getTrades()));
        mockData.setFwDbMsgDbSeq(BusinessTransactionJsonGenerator.generateFwDbXSeqJsonObject());
        mockData.setMultiAssetTransactionLog(BusinessTransactionJsonGenerator.generateMultiAssetTransactionLogJsonObject());
        mockData.setHighestMultiAssetTransactionLog(BusinessTransactionJsonGenerator.generateHighestMultiAssetTransactionLogJsonObject());
        mockData.setDbLatestTxnIdForTrades(BusinessTransactionJsonGenerator.generateDbLatestTxnIdForTradesJsonObject());
        mockData.setFwDbMsgqOutSeq(BusinessTransactionJsonGenerator.generateFwDbMsgqOutSeqJsonObject());

        return mockData;
    }

    private static void simulateBusinessTransaction(BusinessTransactionData businessTransaction) {
        List<Mono<GetResult>> operations = new ArrayList<>();

        operations.add(performDBOperation(businessTransaction.getDbDeal().getString("key"), businessTransaction.getDbDeal()));
        operations.add(performDBOperation(businessTransaction.getTrades().getTradeId(), businessTransaction.getTrades().getTrade()));
        operations.add(performDBOperation(businessTransaction.getTrades().getInstrumentKey(), businessTransaction.getPosition()));
        operations.add(performDBOperation(businessTransaction.getTrades().getInstrumentKey(), businessTransaction.getOpenInterest()));
        operations.add(performDBOperation(businessTransaction.getFwDbMsgDbSeq().getString("key"), businessTransaction.getFwDbMsgDbSeq()));
        operations.add(performDBOperation(businessTransaction.getMultiAssetTransactionLog().getString("key"), businessTransaction.getMultiAssetTransactionLog()));
        operations.add(performDBOperation(businessTransaction.getHighestMultiAssetTransactionLog().getString("key"), businessTransaction.getHighestMultiAssetTransactionLog()));
        operations.add(performDBOperation(businessTransaction.getDbLatestTxnIdForTrades().getString("key"), businessTransaction.getDbLatestTxnIdForTrades()));
        operations.add(performDBOperation(businessTransaction.getFwDbMsgqOutSeq().getString("key"), businessTransaction.getFwDbMsgqOutSeq()));

        Flux.merge(operations).blockLast();  // Wait for all operations to complete
        //Flux.concat(operations).blockLast();
    }

    private static Mono<GetResult> performDBOperation(String key, JsonObject jsonObject) {
        return collection.reactive().upsert(key, jsonObject)
                .flatMap(result -> collection.reactive().get(key))
                .doOnError(e -> System.err.println("Error occurred: " + e.getMessage()));
    }

/*    private static List<MutationResult> simulateMultipleDbDeals(List<JsonObject> data) {

        return Flux.fromIterable(data)
                .flatMap(doc -> collection.reactive().upsert(doc.getString("key"),doc))
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();

    }

    public static List<JsonObject> generateDBDealDataListOfDocuments() {

        return Flux.range(0, 30)
                .map(i -> BusinessTransactionJsonGenerator.generateDBDealJSONData())
                .doOnError(e -> Flux.empty())
                .collectList()
                .block();
    }*/
}

