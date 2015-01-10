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

package java8;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
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
     * @throws InterruptedException
     *             On a failure waiting for a future.
     */
    public static void main(final String[] args) throws IOException,
            InterruptedException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        // Batching requests is accomplished via the BatchedAsyncMongoCollection
        // interface which we get from the startBatch() method. The batch needs
        // to always be closed to submit the requests so we use a
        // try-with-resources.
        final CountDownLatch latch = new CountDownLatch(1);
        try (BatchedAsyncMongoCollection batch = theCollection.startBatch()) {

            // Now we can do as many CRUD operations we want. Even commands like
            // are supported.

            // We need some data. Lets create a documents with the _id field 'a'
            // thru 'z'.
            final DocumentBuilder builder = BuilderFactory.start();
            for (char c = 'a'; c <= 'z'; ++c) {
                builder.reset().add("_id", String.valueOf(c));

                // Lambda is called once the batch completes.
                batch.insertAsync((e, count) -> {
                    // Nothing.
                    }, builder);
            }

            // A query works.
            final Find.Builder find = Find.builder();
            find.query(where("_id").equals("a"));
            batch.findOneAsync((e, found) -> {
                System.out.println("Find 'a': ");
                System.out.println("  " + found);
            }, find);

            // An update too.
            final DocumentBuilder updateDoc = BuilderFactory.start();
            updateDoc.push("$set").add("marked", true);
            batch.updateAsync((e, updated) -> {
                System.out.println("Update all of the documents: " + updated);
            }, Find.ALL, updateDoc, true, false);

            // Delete should work.
            batch.deleteAsync((e, deleted) -> {
                System.out.println("Delete 'b': " + deleted);
            }, where("_id").equals("b"));

            // Commands... It is all there.
            batch.countAsync((e, count) -> {
                System.out.println("Count all documents: " + count);
            }, Find.ALL);

            // Lets look at the 'a' doc one more time. It should have the
            // "marked" field now.
            batch.findOneAsync((e, found) -> {
                System.out.println("Find 'a' after the update: ");
                System.out.println("  " + found);

                latch.countDown();
            }, find);

            // At this point nothing has been sent to the server. All of the
            // messages have been "spooled" waiting to be sent.
            // All of the messages will use the same connection
            // (unless a read preference directs a query to a different
            // server).

        } // Send the batch.

        /**
         * Should produce output like:
         * 
         * <pre>
         * <code>
         * Find 'a':
         *   { '_id' : 'a' }
         * Update all of the documents: 26
         * Delete 'b': 1
         * Count all documents: 25
         * Find 'a' after the update:
         *   {
         *   '_id' : 'a',
         *   marked : true
         * }
         * </code>
         * </pre>
         */

        latch.await();

        // Perfect!

        client.close();
    }
}
