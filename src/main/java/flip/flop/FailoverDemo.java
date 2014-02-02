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

package flip.flop;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;

/**
 * FailoverDemo a demonstration of the driver handling failover of a replica
 * set.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FailoverDemo {

    /** The id for the well known document. */
    public static final ObjectId ID = new ObjectId("CAFEdeadBEEFcafeDEADbeef");

    /** The connection URI. */
    public static final String URI = "mongodb://testdbuser:testdbpass@flip.mongolab.com:53117,flop.mongolab.com:54117/testdb";

    /**
     * Runs the demo to periodically find an modify a document.
     * 
     * @param args
     *            Command line arguments. If the first argument is present then
     *            controls the number of milliseconds between requests. Defaults
     *            to 2 minutes.
     */
    public static void main(String[] args) {

        // Time between requests. Make this shorter to increase the probability
        // of a request when a fail over happens or during a failover.
        long sleep = TimeUnit.MINUTES.toMillis(2);
        if (args.length > 0) {
            sleep = Long.parseLong(args[1]);
        }

        SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        MongoClient client = MongoFactory.createClient(URI);
        MongoClientConfiguration config = client.getConfig();
        config.setMaxConnectionCount(5);
        config.setMinConnectionCount(1);

        // Set closing idle connections to 5 seconds
        // (5, 1 seconds read timeouts in a row).
        config.setReadTimeout((int) TimeUnit.SECONDS.toMillis(1));
        config.setMaxIdleTickCount(5);

        MongoDatabase db = client.getDatabase("testdb");
        MongoCollection collection = db.getCollection("sync_demo");

        for (Document doc : collection.find(Find.ALL)) {
            System.out.println(doc);
        }
        // Look for the document. If it does not exist, create it.
        // If it does exist, increment a counter.
        while (true) {
            try {
                DocumentBuilder update = BuilderFactory.start();
                update.push("$inc").add("count", 1);
                update.push("$set")
                        .add("driver", "Asynchronous Java Driver")
                        .add("url",
                                "http://www.allanbank.com/mongodb-async-driver");

                FindAndModify findAndModify = FindAndModify.builder()
                        .query(where("_id").equals(ID)).update(update)
                        .returnNew().upsert().build();

                // Fire a bunch of requests to get more connections to open.
                //
                // If the closing of idle connections is short enough you will
                // see the driver drop all but the minimum number of connections
                // while still handling the reconnects.
                //
                // Two fail-over scenarios for the request:
                // 1) If a fail-over is already in progress then the driver will
                // block the request until a primary is found up to the
                // configured reconnect timeout.
                // 2) If the request is sent and then a fail-over starts (before
                // the reply is received) then a MongoDbException is thrown.
                List<Future<Document>> futures = new ArrayList<>();
                for (int i = 0; i < config.getMaxConnectionCount() * 3; ++i) {
                    futures.add(collection.findAndModifyAsync(findAndModify));
                }
                for (Future<Document> future : futures) {
                    Document doc = future.get();
                    System.out.println(sdf.format(new Date())
                            + ": Document Updated "
                            + doc.get("count").getValueAsString() + " times!");
                }
                TimeUnit.MILLISECONDS.sleep(sleep);
            }
            catch (MongoDbException error) {
                System.out.println(error.getClass().getSimpleName() + ": "
                        + error.getMessage());
                System.out
                        .println("Last command might have finished, might not have.");

                // Note: There is no sleep here. The driver will block the
                // caller until the reconnect is finished.
            }
            catch (InterruptedException e) {
                System.out.println("Woke up early!");
            }
            catch (ExecutionException error) {
                System.out.println(error.getClass().getSimpleName() + ": "
                        + error.getMessage());
                System.out
                        .println("Last command might have finished, might not have.");
            }
        }
    }
}
