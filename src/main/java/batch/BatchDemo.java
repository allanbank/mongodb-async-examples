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

package batch;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;

/**
 * BatchDemo provides a simple example of batching inserts, updates and deletes
 * using the asynchronous API.
 * <p>
 * Batching of commands is done via the {@link BatchedAsyncMongoCollection}
 * interface.
 * </p>
 * 
 * @see <a
 *      href="http://docs.mongodb.org/master/reference/command/insert/#dbcmd.insert">Insert
 *      Command</a>
 * @see <a
 *      href="http://docs.mongodb.org/master/reference/command/insert/#dbcmd.update">Update
 *      Command</a>
 * @see <a
 *      href="http://docs.mongodb.org/master/reference/command/insert/#dbcmd.delete">Delete
 *      Command</a>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchDemo {
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
     * @throws ExecutionException
     *             On a failure in a future.
     * @throws InterruptedException
     *             On a failure waiting for a future.
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ExecutionException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        // Batching requests is accomplished via the BatchedAsyncMongoCollection
        // interface which we get from the startBatch() method. The batch needs
        // to always be closed to submit the requests so we use a
        // try-with-resources.
        final long start = System.currentTimeMillis();
        List<Future<Integer>> insertResults = new ArrayList<>();
        Future<Document> found = null;
        Future<Long> update = null;
        Future<Long> delete = null;
        Future<Long> count = null;
        Future<Document> found2 = null;
        try (BatchedAsyncMongoCollection batch = theCollection.startBatch()) {

            // Now we can do as many CRUD operations we want. Even commands like
            // are supported.

            // We need some data. Lets create a documents with the _id field 'a'
            // thru 'z'.
            DocumentBuilder builder = BuilderFactory.start();
            for (char c = 'a'; c <= 'z'; ++c) {
                builder.reset().add("_id", String.valueOf(c));

                // Returns a Future that will only complete once the batch
                // completes.
                insertResults.add(batch.insertAsync(builder));
            }

            // A query works.
            Find.Builder find = Find.builder();
            find.query(where("_id").equals("a"));
            found = batch.findOneAsync(find);

            // An update too.
            DocumentBuilder updateDoc = BuilderFactory.start();
            updateDoc.push("$set").add("marked", true);
            update = batch.updateAsync(Find.ALL, updateDoc, true, false);

            // Delete should work.
            delete = batch.deleteAsync(where("_id").equals("b"));

            // Commands... It is all there.
            count = batch.countAsync(Find.ALL);

            // Lets look at the 'a' doc one more time. It should have the
            // "marked" field now.
            found2 = batch.findOneAsync(find);

            // At this point nothing has been sent to the server. All of the
            // messages have been "spooled" waiting to be sent.
            // All of the messages will use the same connection
            // (unless a read preference directs a query to a different
            // server).

            // Lets prove it by waiting (not too long) on the first insert's
            // Future.
            try {
                insertResults.get(0).get(5, TimeUnit.SECONDS);
                System.out.println(duration(start)
                        + " - ERROR: The insert was sent to early...");
            }
            catch (TimeoutException good) {
                System.out
                        .println(duration(start)
                                + " - Good - Timed out waiting for the first insert, before it was sent.");
            }
        } // Send the batch.

        // Check out the results.

        // The inserts...
        System.out.print(duration(start) + " - Inserts: ");
        for (Future<Integer> insert : insertResults) {
            // Just checking for an error.
            insert.get();
        }
        System.out.println(insertResults.size());

        System.out.println(duration(start) + " - Find 'a': ");
        System.out.println("  " + found.get());

        System.out.println(duration(start) + " - Update all of the documents: "
                + update.get());

        System.out.println(duration(start) + " - Delete 'b': " + delete.get()
                + " - " + duration(start));

        // The count should be 1 less than the inserts.
        System.out.println(duration(start) + " - Count all documents: "
                + count.get() + " - " + duration(start));

        System.out.println(duration(start) + " - Find 'a' after the update: ");
        System.out.println("  " + found2.get());

        System.out.println("Total time for demo: " + duration(start));

        /**
         * Should produce output like:
         * 
         * <pre>
         * <code>
         * 5 s - Good - Timed out waiting for the first insert, before it was sent.
         * 5 s - Inserts: 26
         * 5 s - Find 'a': 
         *   { '_id' : 'a' }
         * 5 s - Update all of the documents: 26
         * 5 s - Delete 'b': 1 - 5 s
         * 5 s - Count all documents: 25 - 5 s
         * 5 s - Find 'a' after the update: 
         *   {
         *   '_id' : 'a',
         *   marked : true
         * }
         * Total time for demo: 5 s
         * </code>
         * </pre>
         */

        // Perfect!

        // Always remember to close your client!
        client.close();
    }

    /**
     * Returns a reasonably scopes duration string.
     * 
     * @param start
     *            The start time.
     * @return The duration string.
     */
    private static String duration(long start) {
        long delta = System.currentTimeMillis() - start;

        if (TimeUnit.MILLISECONDS.toMinutes(delta) > 0) {
            return TimeUnit.MILLISECONDS.toMinutes(delta) + " minutes";
        }
        else if (TimeUnit.MILLISECONDS.toSeconds(delta) > 0) {
            return TimeUnit.MILLISECONDS.toSeconds(delta) + " s";
        }
        return delta + " ms";
    }
}
