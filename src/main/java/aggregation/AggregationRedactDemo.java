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

import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.ifNull;
import static com.allanbank.mongodb.builder.expression.Expressions.setIsSubset;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.RedactOption;

/**
 * AggregationCursorDemo provides a simple example of using the aggregation
 * framework to redact portions of a document.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationRedactDemo {

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
        // Before we start lets make sure there is not already a document.
        theCollection.delete(Find.ALL);

        // We need some data with lots of size to make sure we are over the 16MB
        // limit.
        final DocumentBuilder inserted = d(
                // Overall security.
                e("security", a("TS", "SI", "REL")),
                e("name", "something"),
                e("title", "important"),

                // Sub-document without a security element.
                e("unimportant", d(e("no-one", "cares"))),

                // The ECI sub-document.
                e("subdoc",
                        d(e("security", a("TS", "SI", "REL", "ECI")),
                                e("animal", "Lion"))));

        theCollection.insert(inserted);
        for (final Document doc : theCollection.find(Find.ALL)) {
            System.out.println("Inserted : " + doc);
        }

        // Now we can start retrieving the document with the ECI.
        final Aggregate.Builder aggregation = Aggregate.builder();
        aggregation.redact(
                setIsSubset(ifNull(field("security"), constant(a())),
                        constant(a("TS", "SI", "REL", "ECI"))),
                        RedactOption.DESCEND, RedactOption.PRUNE);
        System.out.println("Aggregation Pipeline : " + aggregation);

        for (final Document doc : theCollection.aggregate(aggregation)) {
            System.out.println("All  : " + doc);
        }

        // Someone without ECI but has OTR?
        aggregation.reset().redact(
                setIsSubset(ifNull(field("security"), constant(a())),
                        constant(a("TS", "SI", "REL", "OTR"))),
                        RedactOption.DESCEND, RedactOption.PRUNE);
        for (final Document doc : theCollection.aggregate(aggregation)) {
            System.out.println("No-ECI  : " + doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
