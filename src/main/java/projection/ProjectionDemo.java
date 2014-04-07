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
package projection;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.IOException;
import java.util.Random;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;

/**
 * ProjectionDemo provides a simple example of limiting the fields of a document
 * returned from the database using a {@code projection}.
 * 
 * @see <a
 *      href="http://docs.mongodb.org/manual/core/read-operations/#projections">MongoDB
 *      Query Projections</a>
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/operator/query/#projection-operators">MongoDB
 *      Projection Operators</a>
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ProjectionDemo {
    /**
     * The handle to the MongoDB client. We assume MongoDB is running on your
     * machine on the default port of 27017.
     */
    private final static MongoClient client = MongoFactory
            .createClient("mongodb://localhost:27017/");

    /**
     * A source of no so random values. Use a fixed seed to always get the same
     * values for fields.
     */
    private final static Random random = new Random(123456789L);

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
     */
    public static void main(final String[] args) throws IOException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(where("_id").equals(1));

        // We need some data with lots of fields. Lets create a
        // document with the fields 'a' thru 'z'. We can use a random value for
        // each field since we really don't care what the fields are.
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1); // Well known id.
        for (char c = 'a'; c <= 'z'; ++c) {
            builder.add(String.valueOf(c), random.nextInt());
        }
        theCollection.insert(builder);

        // Now we can start retrieving the document. For that we will need to do
        // a find with a query on the id: { '_id' : 1 };
        final Find.Builder find = Find.builder();
        find.query(where("_id").equals(1));

        // First lets just extract the document with all of the fields.
        Document found = theCollection.findOne(find);

        // ... and the results is:
        System.out.println("The returned document with all fields:");
        System.out.println(found);
        System.out.println();
        /**
         * <pre>
         * <code> 
         * {
         *   '_id' : 1,
         *   a : -1442945365,
         *   b : -1016548095,
         *   ...
         *   y : 823057922,
         *   z : -223996045
         * }
         * </code>
         * </pre>
         */

        // Now for the fun part. Lets ask MongoDB to just send back the 'b', and
        // 'y' fields. This is referred to as a projection in MongoDB.
        find.projection("b", "y");

        found = theCollection.findOne(find);

        // ... and the results is:
        System.out.println("The returned document with 'b', and 'y' fields:");
        System.out.println(found);
        System.out.println();
        /**
         * <pre>
         * <code> 
         * {
         *   '_id' : 1,
         *   b : -1016548095,
         *   y : 823057922
         * }
         * </code>
         * </pre>
         */

        // Hey! Wait a minute. What is the '_id' field doing in the result?
        //
        // Turns out MongoDB really wants you to have the '_id' field in the
        // result. To get rid of it you have to explicitly say you don't want
        // it. That requires us to create a projection document. Use a value
        // of 1 for each field to include and 0 for the '_id' field to exclude
        // it.
        //
        // Lets try again for 'a' and 'z' but this time without '_id'.
        final DocumentBuilder projection = BuilderFactory.start();
        projection.add("_id", 0).add("a", 1).add("z", 1);

        find.projection(projection);

        found = theCollection.findOne(find);

        // ... and the results is:
        System.out.println("The returned document with 'a', and 'z' fields "
                + "without '_id':");
        System.out.println(found);
        System.out.println();
        /**
         * <pre>
         * <code> 
         * {
         *   a : -1442945365,
         *   z : -223996045
         * }
         * </code>
         * </pre>
         */

        // Perfect!

        // Further Reading:
        // http://docs.mongodb.org/manual/core/read-operations/#projections
        // http://docs.mongodb.org/manual/reference/operator/query/#projection-operators

        // Always remember to close your client!
        client.close();
    }
}
