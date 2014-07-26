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
package aggregation.mongodb_user20140625;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.Find.ALL;
import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.AggregationGroupId;

/**
 * Construct a sample aggregation pipeline using the driver's API.
 * 
 * @see <a
 *      href="https://groups.google.com/d/msg/mongodb-user/czcBDNFNBcM/jqqk-c83YawJ">mongodb-user
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
        theCollection.insert(d(e("user_id", 1), e("book_id", 1)));
        theCollection.insert(d(e("user_id", 1), e("book_id", 4)));
        theCollection.insert(d(e("user_id", 2), e("book_id", 5)));
        theCollection.insert(d(e("user_id", 2), e("book_id", 6)));

        // Not included in the match...
        theCollection.insert(d(e("user_id", 99), e("book_id", 1)));
        theCollection.insert(d(e("user_id", 1), e("book_id", 99)));

        // db.appModules.aggregate({
        final Aggregate.Builder builder = new Aggregate.Builder();

        builder.match(where("user_id").greaterThan(0).lessThan(10)
                .and("book_id").greaterThan(0).lessThan(10));

        builder.group(AggregationGroupId.id("user_id"), set("book_id").addToSet("book_id"));
                
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
