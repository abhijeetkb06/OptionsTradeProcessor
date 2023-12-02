package org.couchbase;

import com.couchbase.client.java.*;

import java.time.Duration;

public class CouchbaseConfig {

    //Capella Connection
   /* private static final String CONNECTION_STRING = "couchbases://cb.bgzm40pdb7nphef.cloud.couchbase.com";
    private static final String USERNAME = "abhijeet";
    private static final String PASSWORD = "Password@P1";*/

    // AWS connection
//    private static final String CONNECTION_STRING = "couchbase://ec2-3-143-153-43.us-east-2.compute.amazonaws.com";

    //Local connection
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

    public static Cluster getCluster() {
        return cluster;
    }

    public static Bucket getBucket() {
        return bucket;
    }

    public static Scope getScope() {
        return scope;
    }

    public static Collection getCollection() {
        return collection;
    }
}