/*
 *           Copyright 2014 - Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http: *www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aggregation;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.expression.Expressions.add;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.map;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static com.allanbank.mongodb.builder.expression.Expressions.var;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Find;

/**
 * AggregationMapDemo provides a simple example of using the aggregation
 * framework with a map expression.
 * <p>
 * Inspired by the <a href=
 * "http://docs.mongodb.org/master/reference/operator/aggregation/map/">
 * <code>map</code> expression's documentation</a>.
 * </p>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationMapDemo {
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
     */
    public static void main(final String[] args) throws IOException {
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        /**
         * <pre>
         * <code>
         * Inserted : {
         *   '_id' : 0,
         *   skews : [
         *     1, 
         *     1, 
         *     2, 
         *     3, 
         *     5, 
         *     8
         *   ]
         * }
         * </code>
         * </pre>
         */
        final DocumentBuilder inserted = d(e("_id", 0),
                e("skews", a(1, 1, 2, 3, 5, 8)));
        theCollection.insert(inserted);
        for (final Document doc : theCollection.find(Find.ALL)) {
            System.out.println("Inserted : " + doc);
        }

        /**
         * <pre>
         * <code>
         * { $project: { adjustments: { $map: { input: "$skews",
         *                            as: "adj",
         *                            in: { $add: [ "$$adj", 12 ] } } } } }
         * </code>
         * </pre>
         */
        final Aggregate.Builder aggregation = Aggregate.builder();
        aggregation.project(
                include(),
                set("adjustments",
                        map("skews").as("adj")
                        .in(add(var("adj"), constant(12)))));

        /**
         * <pre>
         * <code>
         * Aggregation Pipeline : '$pipeline' : [
         *    {
         *       '$project' : {
         *          adjustments : {
         *             '$map' {
         *                input : '$skews',
         *                as : 'adj',
         *                in : {
         *                   '$add' : [
         *                      '$$adj',
         *                      12
         *                   ]
         *                }
         *             }
         *          }
         *       }
         *    }
         * ]
         * </code>
         * </pre>
         */
        System.out.println("Aggregation Pipeline : " + aggregation);

        /**
         * <pre>
         * <code>
         * Results  : {
         *   '_id' : 0,
         *   adjustments : [
         *     13, 
         *     13, 
         *     14, 
         *     15, 
         *     17, 
         *     20
         *   ]
         * }
         * </code>
         * </pre>
         */
        for (final Document doc : theCollection.aggregate(aggregation)) {
            System.out.println("Results  : " + doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
