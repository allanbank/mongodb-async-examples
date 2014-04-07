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
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;

/**
 * SecondaryQueryDemo provides a demo of querying a replica set in fail over.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SecondaryQueryDemo {

    /** The id for the well known document. */
    public static final ObjectId ID = new ObjectId("CAFEdeadBEEFcafeDEADbeef");

    /** The connection URI. */
    public static final String URI = "mongodb://testdbuser:testdbpass@flip.mongolab.com:53117,flop.mongolab.com:54117/testdb";

    /**
     * Runs the demo to query for the document.
     * 
     * @param args
     *            Command line arguments. If the first argument is present then
     *            controls the number of milliseconds between requests. Defaults
     *            to 0.1 seconds.
     */
    public static void main(final String[] args) {

        long sleep = 100;
        if (args.length > 0) {
            sleep = Long.parseLong(args[1]);
        }

        final SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        final MongoClient client = MongoFactory.createClient(URI);
        final MongoClientConfiguration config = client.getConfig();

        // Set closing idle connections to 5 minutes
        // (5, 1 minute read timeouts in a row).
        config.setReadTimeout((int) TimeUnit.MINUTES.toMillis(1));
        config.setMaxIdleTickCount(5);

        config.setMaxConnectionCount(5);
        config.setMinConnectionCount(1);

        final MongoDatabase db = client.getDatabase("testdb");
        final MongoCollection collection = db.getCollection("sync_demo");
        for (final Document doc : collection.find(Find.ALL)) {
            System.out.println(doc);
        }

        // Look for the document.
        // If we used PREFER_PRIMARY here then it is more likely to see
        // exceptions when the primary steps down and closes the active
        // connections. We also will see pauses due to the driver trying
        // to reconnect to the primary.
        final Find query = Find.builder().query(where("_id").equals(ID))
                .readPreference(ReadPreference.PREFER_SECONDARY).build();
        Document lastDocument = null;
        int count = 0;
        while (true) {
            try {
                final Document document = collection.findOne(query);

                // Print dots if the document does not change. The whole
                // document if it does.
                if (document.equals(lastDocument)) {
                    System.out.print('.');
                    count += 1;
                    if ((count % 40) == 0) {
                        System.out.println();
                        System.out.print(sdf.format(new Date()) + ": ");
                    }
                }
                else {
                    lastDocument = document;
                    if (count != 0) {
                        System.out.println();
                    }
                    System.out
                    .println(sdf.format(new Date()) + ": " + document);
                    count = 0;
                }
                TimeUnit.MILLISECONDS.sleep(sleep);
            }
            catch (final MongoDbException error) {
                System.out.println();
                System.out.println(error.getClass().getSimpleName() + ": "
                        + error.getMessage());

                // Note: There is no sleep here. The driver will block the
                // caller until the reconnect is finished.
            }
            catch (final InterruptedException e) {
                System.out.println();
                System.out.println("Woke up early!");
            }
        }
    }
}
