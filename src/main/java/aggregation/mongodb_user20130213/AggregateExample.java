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
package aggregation.mongodb_user20130213;

import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * Construct a sample aggregation pipeline using the driver's API.
 * <p>
 * Sample output of the application is:<blockquote>
 * 
 * <pre>
 * <code>
 * pipeline : [
 *   { '$unwind' : '$dataItems' }, 
 *   {
 *     '$match' : {
 *       'dataItems.version' : { '$gt' : 0 }
 *     }
 *   }, 
 *   {
 *     '$project' : {
 *       dataItems : 1,
 *       proc1 : '$dataItems.version',
 *       proc2 : '$dataItems.data'
 *     }
 *   }
 * ]
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @see <a
 *      href="https://groups.google.com/d/topic/mongodb-user/Z8x5V3Ky17g/discussion">mongodb-user
 *      question</a>
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
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
    public static void main(String[] args) throws IOException {
        // db.appModules.aggregate({
        Aggregate.Builder builder = new Aggregate.Builder();
        // $unwind:"$dataItems"},
        builder.unwind("dataItems");
        // {$match:{"dataItems.version":{$gt:0}}},
        builder.match(where("dataItems.version").greaterThan(0));
        // {$project: {dataItems:1, proc1:"$dataItems.version",
        // proc2:"$dataItems.data"}});
        builder.project(include("dataItems"),
                set("proc1", field("dataItems.version")),
                set("proc2", field("dataItems.data")));

        System.out.println(builder);

        Iterable<Document> docs = theCollection.aggregate(builder.build());
        for (Document doc : docs) {
            System.out.println(doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
