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

package aggregation;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.expression.Expressions.add;
import static com.allanbank.mongodb.builder.expression.Expressions.cond;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.let;
import static com.allanbank.mongodb.builder.expression.Expressions.multiply;
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
 * framework with a {@code $let} expression.
 * <p>
 * Inspired by the <a href=
 * "http://docs.mongodb.org/master/reference/operator/aggregation/let/">
 * <code>let</code> expression's documentation</a>.
 * </p>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationLetDemo {
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
         * Inserted : 
         * {
         *   '_id' : 1,
         *   price : 10,
         *   tax : 0.5,
         *   applyDiscount : true
         * }
         * {
         *   '_id' : 2,
         *   price : 10,
         *   tax : 0.25,
         *   applyDiscount : false
         * }
         * </code>
         * </pre>
         */
        // { _id: 1, price: 10, tax: 0.50, applyDiscount: true }
        // { _id: 2, price: 10, tax: 0.25, applyDiscount: false }
        DocumentBuilder inserted = d(e("_id", 1), e("price", 10),
                e("tax", 0.50), e("applyDiscount", true));
        theCollection.insert(inserted);
        inserted = d(e("_id", 2), e("price", 10), e("tax", 0.25),
                e("applyDiscount", false));
        theCollection.insert(inserted);
        System.out.println("Inserted : ");
        for (final Document doc : theCollection.find(Find.ALL)) {
            System.out.println(doc);
        }

        /**
         * <pre>
         * <code>
         *   $project: {
         *      finalTotal: {
         *         $let: {
         *            vars: {
         *               total: { $add: [ '$price', '$tax' ] },
         *               discounted: { $cond: { if: '$applyDiscount', then: 0.9, else: 1 } }
         *            },
         *            in: { $multiply: [ "$$total", "$$discounted" ] }
         *         }
         *      }
         *   }
         * </code>
         * </pre>
         */
        final Aggregate.Builder aggregation = Aggregate.builder();
        aggregation.project(
                include(),
                set("finalTotal",
                        let("total", add(field("price"), field("tax"))).let(
                                "discounted",
                                cond(field("applyDiscount"), constant(0.9),
                                        constant(1))).in(
                                multiply(var("total"), var("discounted")))));

        /**
         * <pre>
         * <code>
         * Aggregation Pipeline : '$pipeline' : [
         *   {
         *     '$project' : {
         *       finalTotal : {
         *         '$let' : {
         *           vars : {
         *             total : {
         *               '$add' : [
         *                 '$price', 
         *                 '$tax'
         *               ]
         *             },
         *             discounted : {
         *               '$cond' : [
         *                 '$applyDiscount', 
         *                 0.9, 
         *                 1
         *               ]
         *             }
         *           },
         *           in : {
         *             '$multiply' : [
         *               '$$total', 
         *               '$$discounted'
         *             ]
         *           }
         *         }
         *       }
         *     }
         *   }
         * ]
         * </code>
         * </pre>
         */
        System.out.println("Aggregation Pipeline : " + aggregation);

        /**
         * <pre>
         * <code>
         * Results  : 
         * {
         *   '_id' : 1,
         *   finalTotal : 9.450000000000001
         * }
         * {
         *   '_id' : 2,
         *   finalTotal : 10.25
         * }
         * </code>
         * </pre>
         */
        System.out.println("Results  : ");
        for (final Document doc : theCollection.aggregate(aggregation)) {
            System.out.println(doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
