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

package aggregation;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Find;

/**
 * AggregationCursorDemo provides a simple example of using the aggregation
 * framework with cursors.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationCursorDemo {

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
    public static void main(final String[] args) throws IOException,
    InterruptedException, ExecutionException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        // We need some data with lots of size to make sure we are over the 16MB
        // limit.
        final int inserted = 100_000;
        final DocumentBuilder builder = BuilderFactory.start();
        for (int i = 0; i < inserted; i += 1) {
            builder.reset().add("_id", i);
            for (char c = 'a'; c <= 'z'; ++c) {
                builder.add("text_" + String.valueOf(c),
                        "Now is the time for all good men to come to the aid of thier country.");
            }
            theCollection.insert(builder);
        }

        // Now we can start retrieving the document. For that we will need to do
        // a find with a query on the id: { '_id' : 1 };
        final Aggregate.Builder aggregation = Aggregate.builder();
        aggregation.useCursor();
        aggregation.match(Find.ALL);

        // First lets just extract the document with all of the fields.
        int count = 0;
        final MongoIterator<Document> iter = theCollection
                .aggregate(aggregation);
        while (iter.hasNext()) {
            iter.next();
            count += 1;
        }

        // ... and the results is:
        System.out
        .println("Found " + count + " of " + inserted + " documents.");

        // Always remember to close your client!
        client.close();
    }
}
