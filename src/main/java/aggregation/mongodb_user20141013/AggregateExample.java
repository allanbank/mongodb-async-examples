/*
 *           Copyright 2014 - Allanbank Consulting, Inc.
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
package aggregation.mongodb_user20141013;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.Find.ALL;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.expression.Expressions.add;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.multiply;
import static com.allanbank.mongodb.builder.expression.Expressions.set;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * Construct a sample aggregation pipeline using the driver's API.
 * 
 * @see <a
 *      href="https://groups.google.com/forum/#!topic/mongodb-user/7Yq-rOYB0N4">mongodb-user
 *      question</a>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregateExample {
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
     *             On a failure closing the MongoClient.
     */
    public static void main(final String[] args) throws IOException {
        // Clear the collection.
        theCollection.delete(ALL);

        // Add the test data.
        final long now = System.currentTimeMillis();
        theCollection
                .insert(d(
                        e("name", "alice"),
                        e("interval", 5),
                        e("lastTimeUpdated",
                                new Date(now - TimeUnit.HOURS.toMillis(5)))));
        theCollection
                .insert(d(
                        e("name", "bob"),
                        e("interval", 6),
                        e("lastTimeUpdated",
                                new Date(now - TimeUnit.HOURS.toMillis(4)))));
        theCollection
                .insert(d(
                        e("name", "charlie"),
                        e("interval", 3),
                        e("lastTimeUpdated",
                                new Date(now - TimeUnit.HOURS.toMillis(3)))));
        theCollection
                .insert(d(
                        e("name", "david"),
                        e("interval", 7),
                        e("lastTimeUpdated",
                                new Date(now - TimeUnit.HOURS.toMillis(2)))));

        // db.collection.aggregate({
        final Aggregate.Builder builder = new Aggregate.Builder();

        builder.project(
                include("name", "interval", "lastTimeUpdated"),
                set("deadline",
                        add(field("lastTimeUpdated"),
                                multiply(field("interval"),
                                        constant(TimeUnit.HOURS.toMillis(1))))));

        builder.match(where("deadline").lessThan(new Date()));

        // Optional projection to remove the synthetic field.
        builder.project(include("name", "interval", "lastTimeUpdated"));

        System.out.println(builder);

        final Iterable<Document> docs = theCollection
                .aggregate(builder.build());
        for (final Document doc : docs) {
            System.out.println(doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
