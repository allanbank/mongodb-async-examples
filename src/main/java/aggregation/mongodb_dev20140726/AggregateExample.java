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
package aggregation.mongodb_dev20140726;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.Find.ALL;
import static com.allanbank.mongodb.builder.Sort.desc;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;

import java.io.IOException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * Example using the MongoDB Aggregation pipeline to select a document with the
 * highest version based on a different field. In this case we are looking for
 * the highest version of a document with a specific 'fileID'.
 * <p>
 * Input collection: <blockquote>
 * 
 * <pre>
 * <code>
 * { "_id" : A, "fileID" : 0, "size" : 126, "version" : 1}
 * { "_id" : B, "fileID" : 1, "size" : 126, "version" : 1}
 * { "_id" : C, "fileID" : 2, "size" : 121, "version" : 1}
 * { "_id" : D, "fileID" : 1, "size" : 124, "version" : 2}
 * { "_id" : E, "fileID" : 3, "size" : 125, "version" : 2}
 * { "_id" : F, "fileID" : 2, "size" : 120, "version" : 3}
 * { "_id" : G, "fileID" : 4, "size" : 122, "version" : 3}
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * Desired output: <blockquote>
 * 
 * <pre>
 * <code>
 * { "_id" : G, "fileID" : 4, "size" : 122, "version" : 3}
 * { "_id" : E, "fileID" : 3, "size" : 125, "version" : 2}
 * { "_id" : F, "fileID" : 2, "size" : 120, "version" : 3}
 * { "_id" : D, "fileID" : 1, "size" : 124, "version" : 2}
 * { "_id" : A, "fileID" : 0, "size" : 126, "version" : 1}
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * Sample output is:<blockquote>
 * 
 * <pre>
 * <code>
 * Pipeline:
 * '$pipeline' : [
 *   {
 *     '$sort' : {
 *       fileID : -1,
 *       version : -1
 *     }
 *   }, 
 *   {
 *     '$group' : {
 *       '_id' : '$fileID',
 *       id : { '$first' : '$_id' },
 *       version : { '$first' : '$version' },
 *       size : { '$first' : '$size' }
 *     }
 *   }, 
 *   {
 *     '$project' : {
 *       version : 1,
 *       size : 1,
 *       '_id' : '$id',
 *       fileID : '$_id'
 *     }
 *   }
 * ]
 * 
 * Output:
 * {
 *   '_id' : 'A',
 *   version : 1,
 *   size : 126,
 *   fileID : 0
 * }
 * {
 *   '_id' : 'D',
 *   version : 2,
 *   size : 124,
 *   fileID : 1
 * }
 * {
 *   '_id' : 'F',
 *   version : 3,
 *   size : 120,
 *   fileID : 2
 * }
 * {
 *   '_id' : 'E',
 *   version : 2,
 *   size : 125,
 *   fileID : 3
 * }
 * {
 *   '_id' : 'G',
 *   version : 3,
 *   size : 122,
 *   fileID : 4
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @see <a
 *      href="https://groups.google.com/forum/#!topic/mongodb-dev/Rre0KQqC9wM">mongodb-dev
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

        // Empty the collection.
        theCollection.delete(ALL);

        // Add the example documents.
        theCollection.insert(
                d(e("_id", "A"), e("fileID", 0), e("size", 126),
                        e("version", 1)),
                d(e("_id", "B"), e("fileID", 1), e("size", 126),
                        e("version", 1)),
                d(e("_id", "C"), e("fileID", 2), e("size", 121),
                        e("version", 1)),
                d(e("_id", "D"), e("fileID", 1), e("size", 124),
                        e("version", 2)),
                d(e("_id", "E"), e("fileID", 3), e("size", 125),
                        e("version", 2)),
                d(e("_id", "F"), e("fileID", 2), e("size", 120),
                        e("version", 3)),
                d(e("_id", "G"), e("fileID", 4), e("size", 122),
                        e("version", 3)));

        // Now the aggregation pipeline.
        //
        // The idea is to scan through the items in order of the
        // fileID and version. Then use a $group to select the first item.
        //
        // We have to do a final project to reform the documents after the
        // group.

        final Aggregate.Builder builder = new Aggregate.Builder();

        // Setup an initial find and sort. The sort should enable the
        // group to not have to hold "all" of the documents since it knows the
        // documents are being delivered in sorted order.
        builder.sort(desc("fileID"), desc("version"));

        // We really want an index to support that sort.
        theCollection.createIndex(desc("fileID"), desc("version"));

        // Back to the aggregation.
        // Now group on fileID and and grab the first document we see for each
        // fileID.
        builder.group(id("fileID"), set("id").first("_id"), set("version")
                .first("version"), set("size").first("size"));

        // At this stage the documents has the fileId as the _id and _id as the
        // 'id'. Use a projection to fix that.
        builder.project(include("version", "size"), set("_id", field("id")),
                set("fileID", field("_id")));

        // Print the pipeline.
        System.out.println("Pipeline:");
        System.out.println(builder);

        // Un-comment to see the plan for the aggregation.
        // System.out.println(theCollection.explain(builder));

        // The results.
        System.out.println();
        System.out.println("Output:");
        final Iterable<Document> docs = theCollection.aggregate(builder);
        for (final Document doc : docs) {
            System.out.println(doc);
        }

        // Always remember to close your client!
        client.close();
    }
}
