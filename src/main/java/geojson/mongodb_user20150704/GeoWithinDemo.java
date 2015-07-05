/*
 * #%L
 * GeoWithinDemo.java - mongodb-async-examples - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2015 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package geojson.mongodb_user20150704;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static java.util.Arrays.asList;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Index;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * GeoWithinDemo provides an example of creating a GeoWithin query.
 * 
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/operator/query/geoWithin/">geoWithin
 *      Operator</a>
 * @see <a
 *      href="http://www.allanbank.com/mongodb-async-driver/apidocs/com/allanbank/mongodb/builder/GeoJson.html">GeoJson
 *      Helper</a>
 * @see <a
 *      href="http://groups.google.com/group/mongodb-user/t/74d2ea740d71a9cc">mongodb_user
 *      thread</a>
 * 
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GeoWithinDemo {
    /**
     * The handle to the MongoDB client. We assume MongoDB is running on your
     * machine on the default port of 27017.
     */
    private final static MongoClient client = MongoFactory
            .createClient("mongodb://localhost:27017/");

    /** The collection we will be using. */
    private final static MongoCollection theCollection = client.getDatabase(
            "db").getCollection("collection");

    /**
     * Run the demo.
     * 
     * @param args
     *            Command line arguments. Ignored.
     * @throws IOException
     *             On a failure closing the MongoCLient.
     * @throws InterruptedException
     *             On a failure waiting for a future.
     */
    public static void main(final String[] args) throws IOException,
            InterruptedException {
        runDemoWithAsyncDriver();
        runDemoWithLegacyDriver();
    }

    /**
     * Run the demo.
     * 
     * @throws IOException
     *             On a failure closing the MongoCLient.
     * @throws InterruptedException
     *             On a failure waiting for a future.
     */
    public static void runDemoWithAsyncDriver() throws IOException,
            InterruptedException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        // Insert a few documents.
        theCollection.insert(d(e("loc", a(0, 0))));
        theCollection.insert(d(e("loc", a(10, 10))));
        theCollection.insert(d(e("loc", a(-10, 10))));
        theCollection.insert(d(e("loc", a(10, -10))));
        theCollection.insert(d(e("loc", a(-10, -10))));
        theCollection.insert(d(e("loc", a(40, 40))));
        theCollection.insert(d(e("loc", a(-40, 40))));
        theCollection.insert(d(e("loc", a(40, -40))));
        theCollection.insert(d(e("loc", a(-40, -40))));
        theCollection.insert(d(e("loc", a(100, 100))));
        theCollection.insert(d(e("loc", a(-100, 100))));
        theCollection.insert(d(e("loc", a(100, -100))));
        theCollection.insert(d(e("loc", a(-100, -100))));

        // Speed up the query.
        theCollection.createIndex(Index.geo2d("loc"));

        // Create the query.
        DocumentBuilder query = d(e(
                "loc",
                d(e("$geoWithin",
                        d(e("$centerSphere", a(a(40, -40), 10 / 3963.2)))))));
        try (MongoIterator<Document> iter = theCollection.find(query)) {
            for (Document result : iter) {
                System.out.println(result);
            }
        }

        // Always close the client.
        client.close();
    }

    /**
     * Run the demo.
     * 
     * @throws IOException
     *             On a failure closing the MongoCLient.
     * @throws InterruptedException
     *             On a failure waiting for a future.
     */
    public static void runDemoWithLegacyDriver() throws IOException,
            InterruptedException {
        com.mongodb.MongoClient client2 = new com.mongodb.MongoClient(
                "localhost", 27017);
        DBCollection collection = client2.getDB("db").getCollection(
                "collection");

        // Before we start lets make sure there is not already a document.
        collection.remove(new BasicDBObject());

        // Insert a few documents.
        collection.insert(new BasicDBObject("loc", new int[] { 0, 0 }));
        collection.insert(new BasicDBObject("loc", new int[] { 10, 10 }));
        collection.insert(new BasicDBObject("loc", new int[] { -10, 10 }));
        collection.insert(new BasicDBObject("loc", new int[] { 10, -10 }));
        collection.insert(new BasicDBObject("loc", new int[] { -10, -10 }));
        collection.insert(new BasicDBObject("loc", new int[] { 40, 40 }));
        collection.insert(new BasicDBObject("loc", new int[] { -40, 40 }));
        collection.insert(new BasicDBObject("loc", new int[] { 40, -40 }));
        collection.insert(new BasicDBObject("loc", new int[] { -40, -40 }));
        collection.insert(new BasicDBObject("loc", new int[] { 100, 100 }));
        collection.insert(new BasicDBObject("loc", new int[] { -100, 100 }));
        collection.insert(new BasicDBObject("loc", new int[] { 100, -100 }));
        collection.insert(new BasicDBObject("loc", new int[] { -100, -100 }));

        // Speed up the query.
        collection.createIndex(new BasicDBObject("loc", "2d"));

        // Create the query.
        BasicDBObject geometery = new BasicDBObject("$centerSphere", asList(
                asList(40, -40), 10 / 3963.2));
        BasicDBObject operator = new BasicDBObject("$geoWithin", geometery);
        BasicDBObject query = new BasicDBObject("loc", operator);
        try (DBCursor iter = collection.find(query)) {
            for (DBObject result : iter) {
                System.out.println(result);
            }
        }

        // Always close the client.
        client2.close();
    }
}
