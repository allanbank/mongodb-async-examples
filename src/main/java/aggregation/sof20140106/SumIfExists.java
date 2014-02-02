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
package aggregation.sof20140106;

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.constantId;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.Find.ALL;
import static com.allanbank.mongodb.builder.expression.Expressions.cond;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.eq;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.ifNull;
import static com.allanbank.mongodb.builder.expression.Expressions.set;

import java.io.IOException;
import java.util.Random;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * Count the number of documents that have a particular field.
 * <p>
 * This is an interesting problem because the set of operators that can be used
 * within a projection is limited. In this solution we use {@code $ifNull}
 * operator to detect the condition and the use a {@code $eq} within a
 * {@code $cond} to actually set the value.
 * </p>
 * <p>
 * The main take away from trying to solve this problem is that the MongoDB
 * query engine does not behave the same as the expression engine.
 * </p>
 * <p>
 * Sample output of the application is:<blockquote>
 * 
 * <pre>
 * <code>
 * '$pipeline' : [
 *   {
 *     '$project' : {
 *       a : 1,
 *       b : 1,
 *       c : 1,
 *       etc : 1,
 *       myfieldnameExists : {
 *         '$cond' : [
 *           {
 *             '$eq' : [
 *               {
 *                 '$ifNull' : [
 *                   '$myfieldname', 
 *                   ObjectId('52cb9a7a6c4a281ac27e482c')
 *                 ]
 *               }, 
 *               ObjectId('52cb9a7a6c4a281ac27e482c')
 *             ]
 *           }, 
 *           0, 
 *           1
 *         ]
 *       }
 *     }
 *   }, 
 *   {
 *     '$group' : {
 *       '_id' : 'a',
 *       count : { '$sum' : '$myfieldnameExists' }
 *     }
 *   }
 * ]
 * [{
 *   '_id' : 'a',
 *   count : 33
 * }]
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @see <a
 *      href="http://stackoverflow.com/questions/20964169/mongo-java-conditional-sum-if-exists">StackOverflow
 *      Question</a>
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SumIfExists {
    /**
     * A source of no so random values. Use a fixed seed to always get the same
     * values for fields.
     */
    private final static Random random = new Random(123456789L);

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
    public static void main(String[] args) throws IOException {
        // Build the aggregation document/command.
        Aggregate.Builder builder = Aggregate.builder();

        // From the StackOverflow Question.
        String fieldName = "myfieldname";

        // A token ObjectId to use in comparisons for the null field.
        ObjectId nullToken = new ObjectId();

        builder.project(
                include("a", "b", "c", "etc"),
                set("myfieldnameExists",
                        cond(eq(ifNull(field(fieldName), constant(nullToken)),
                                constant(nullToken)), constant(0), constant(1))));
        builder.group(constantId("a"), set("count").sum("myfieldnameExists"));

        // Print the pipeline for review.
        System.out.println(builder);

        // Insert some documents to test with.
        theCollection.delete(ALL);
        for (int i = 0; i < 99; ++i) {
            DocumentBuilder doc = BuilderFactory.start();
            if (i % 3 == 0) {
                doc.addNull(fieldName);
            }
            else if (i % 3 == 1) {
                doc.add(fieldName, random.nextDouble());
            }
            // else if (i % 3 == 2) -- Field does not exist.

            doc.add("a", random.nextBoolean());
            doc.add("b", random.nextInt());
            doc.add("c", random.nextLong());
            doc.add("etc", random.nextLong());

            theCollection.insert(doc);
        }

        // Run the aggregation.
        System.out.println(theCollection.aggregate(builder).toList());

        // Always remember to close your client!
        client.close();
    }
}
