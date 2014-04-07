/*
 *           Copyright 2013 - Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geojson.sof20181050;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.GeoJson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;

/**
 * ConvertToGeoJSON provides a solution to a Stack Overflow question on how to
 * conver a document containing a latitude and longitude value to contain a
 * GeoJSON formated value instead. From the Stack Overflow question: Each input
 * document looks like: <blockquote>
 * 
 * <pre>
 * <code>
 *    {
 *             "_id" : ObjectId("528e8134556062edda12ffe6"),
 *             "id" : 6523,
 *             "ident" : "00A",
 *             "type" : "heliport",
 *             "name" : "Total Rf Heliport",
 *             "latitude_deg" : 40.07080078125,
 *             "longitude_deg" : -74.9336013793945,
 *             "elevation_ft" : 11,
 *             "continent" : "NA",
 *             "iso_country" : "US",
 *             "iso_region" : "US-PA",
 *             "municipality" : "Bensalem",
 *             "scheduled_service" : "no",
 *             "gps_code" : "00A",
 *             "iata_code" : "",
 *             "local_code" : "00A",
 *             "home_link" : "",
 *             "wikipedia_link" : "",
 *             "keywords" : ""
 *     }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * <p>
 * We want the resulting document to look like:<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *       "_id": ObjectId("528e8134556062edda12ffe6"),
 *       "id" : 6523,
 *       "ident" : "00A",
 *       "type" : "heliport",
 *       "name" : "Total Rf Heliport",
 *       "longitude_deg" : 17.27,
 *       "latitude_deg" : 52.22,
 *       "loc" : {
 *         "type" : "Point",
 *         "coordinates" : [
 *           17.27,
 *           52.22
 *         ]
 *       },
 *     ...
 *     }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * 
 * @see <a
 *      href="http://stackoverflow.com/questions/20181050/how-do-i-update-fields-of-documents-in-mongo-db-using-the-java-to-geojson-format">Stack
 *      Overflow Question</a>
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConvertToGeoJSON {
    /**
     * The handle to the MongoDB client. We assume MongoDB is running on your
     * machine on the default port of 27017.
     */
    protected final static MongoClient client;

    /** The collection we will be using. */
    protected final static MongoCollection theCollection;

    /** The URI for connecting to MongoDB. */
    protected static final String URI;

    static {
        URI = "mongodb://localhost:27017/";
        client = MongoFactory.createClient(URI);
        theCollection = client.getDatabase("db").getCollection("collection");
    }

    /**
     * See the class Javadoc for a description of the problem..
     * 
     * @param args
     *            Command line arguments. Ignored.
     * @throws InterruptedException
     *             If waiting for the callback to complete is interrupted.
     * @throws IOException
     *             On a failure to close the client.
     */
    public static void main(final String[] args) throws InterruptedException,
    IOException {
        try {
            // We can perform this operation two way. Synchronously and via
            // streaming. We will provide an example of both. Change these
            // variables to switch between them.
            final boolean doLegacy = false;
            final boolean doSynchronous = false;
            if (doLegacy) {
                doLegacy();
            }
            else if (doSynchronous) {
                doSynchronously();
            }
            else {
                doAsynchronously();
            }
        }
        finally {
            // Always close the client!
            client.close();
        }
    }

    /**
     * Performs the document updates asynchronously. This method uses a pair of
     * callbacks to perform the updates. The first receives the stream of
     * documents in the collection and prepares and sends the update for each
     * document. The second receives the results of the update and checks for
     * errors.
     * <p>
     * Neither callback performs robust error handling but could be easily
     * modified to retry operations etc.
     * </p>
     * <p>
     * The advantage of this version is that the stream of updates can be
     * handled concurrently with the iteration over the results in each batch of
     * documents in the collection. This should result in a significant
     * reduction in the wall clock time for processing the collections time.
     * </p>
     * <p>
     * We use {@link Phaser} instances to track when we are waiting for
     * asynchronous operations so the main thread knows when to terminate the
     * application.
     * </p>
     * 
     * @throws InterruptedException
     *             If waiting for the callback to complete is interrupted.
     */
    protected static void doAsynchronously() throws InterruptedException {
        // Execute the query to find all of the documents and stream
        // them to the callback. Have that callback update the document
        // asynchronously.
        final Phaser finished = new Phaser(1); // Parent.
        final AtomicLong updates = new AtomicLong(0);
        final StreamCallback<Document> streamCallback = new StreamCallback<Document>() {

            // Child Phaser for the complete stream and all updates.
            final Phaser streamFinished = new Phaser(finished, 1);

            @Override
            public void callback(final Document doc) {
                final Element id = doc.get("_id");
                final NumericElement lat = doc.get(NumericElement.class,
                        "latitude_deg");
                final NumericElement lon = doc.get(NumericElement.class,
                        "longitude_deg");

                final DocumentBuilder query = BuilderFactory.start();
                query.add(id);

                final DocumentBuilder update = BuilderFactory.start();
                update.push("$set").add(
                        "loc",
                        GeoJson.point(GeoJson.p(lon.getDoubleValue(),
                                lat.getDoubleValue())));

                final Callback<Long> updateCallback = new Callback<Long>() {

                    // Child Phaser for the update.
                    final Phaser updateFinished = new Phaser(streamFinished, 1);

                    @Override
                    public void callback(final Long result) {
                        // All done. Notify the stream.
                        updates.addAndGet(result);
                        updateFinished.arriveAndDeregister();
                    }

                    @Override
                    public void exception(final Throwable thrown) {
                        System.err.printf("Update of {0} failed.\n", id);
                        thrown.printStackTrace();
                        callback(null);
                    }
                };

                theCollection.updateAsync(updateCallback, query, update,
                        Durability.ACK);
            }

            @Override
            public synchronized void done() {
                streamFinished.arriveAndDeregister();
            }

            @Override
            public void exception(final Throwable thrown) {
                thrown.printStackTrace();
                done();
            }
        };

        // Now to kick off the processing.
        theCollection.streamingFind(streamCallback, Find.ALL);

        // Need to wait for the stream and updates to finish.
        finished.arriveAndAwaitAdvance();

        System.out.printf("Updated %d documents asynchronously.%n",
                updates.get());
    }

    /**
     * Performs the document updates using the legacy driver.
     * <p>
     * The main draw back here (other than those discussed in
     * {@link #doSynchronously()}) is the difficulty creating the GeoJSON
     * documents.
     * </p>
     * 
     * @throws UnknownHostException
     *             On an invalid URI.
     */
    protected static void doLegacy() throws UnknownHostException {
        // Execute the query to find all of the documents and then
        // update them.
        final com.mongodb.MongoClient legacyClient = new com.mongodb.MongoClient(
                new MongoClientURI(URI));
        final com.mongodb.DBCollection legacyCollection = legacyClient.getDB(
                theCollection.getDatabaseName()).getCollection(
                        theCollection.getName());
        try {
            int count = 0;
            for (final DBObject doc : legacyCollection.find()) {
                final Object id = doc.get("_id");
                final Number lat = (Number) doc.get("latitude_deg");
                final Number lon = (Number) doc.get("longitude_deg");

                final BasicDBObject query = new BasicDBObject();
                query.append("_id", id);

                final ArrayList<Double> coordinates = new ArrayList<>();
                coordinates.add(lon.doubleValue());
                coordinates.add(lat.doubleValue());
                final BasicDBObject geojson = new BasicDBObject("type", "Point");
                geojson.append("coordinates", coordinates);
                final BasicDBObject set = new BasicDBObject("loc", geojson);
                final BasicDBObject update = new BasicDBObject("$set", set);

                legacyCollection.update(query, update, /* upsert= */false,
                        /* multi= */false, WriteConcern.ACKNOWLEDGED);

                count += 1;
            }
            System.out.printf("Updated %d documents via the legacy driver.%n",
                    count);
        }
        finally {
            // Always close the client.
            legacyClient.close();
        }
    }

    /**
     * Performs the document updates synchronously.
     * <p>
     * While this version is conceptually easier to implement the need to wait
     * for each update to complete before processing the next document has a
     * severe impact on the wall clock time required to complete the update all
     * but the smallest of documents.
     */
    protected static void doSynchronously() {
        // Execute the query to find all of the documents and then
        // update them.
        int count = 0;
        for (final Document doc : theCollection.find(Find.ALL)) {
            final Element id = doc.get("_id");
            final NumericElement lat = doc.get(NumericElement.class,
                    "latitude_deg");
            final NumericElement lon = doc.get(NumericElement.class,
                    "longitude_deg");

            final DocumentBuilder query = BuilderFactory.start();
            query.add(id);

            final DocumentBuilder update = BuilderFactory.start();
            update.push("$set").add(
                    "loc",
                    GeoJson.point(GeoJson.p(lon.getDoubleValue(),
                            lat.getDoubleValue())));

            theCollection.update(query, update, Durability.ACK);

            count += 1;
        }
        System.out.printf("Updated %d documents synchronously.%n", count);
    }
}
