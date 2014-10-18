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
package async.mongodb_user20141018;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.Find.ALL;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.BatchedWrite;

/**
 * Construct a sample asynchronous streaming find using the driver's API.
 * 
 * @see <a
 *      href="https://groups.google.com/d/msg/mongodb-user/hRQ15Yv_0h4/Y4DDgabpJuAJ">mongodb-user
 *      question</a>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StreamingAsyncExample {
    /**
     * The handle to the MongoDB client. We assume MongoDB is running on your
     * machine on the default port of 27017.
     */
    private final static MongoClient client = MongoFactory
            .createClient("mongodb://localhost:27017/");

    /** The count of documents received. */
    protected static int count = 0;

    /** The latch to signal that the demo is complete. */
    protected final static CountDownLatch doneLatch = new CountDownLatch(1);

    /** The collection we will be using. */
    protected final static MongoCollection theCollection = client.getDatabase(
            "db").getCollection("collection");

    /**
     * Run the demo.
     * 
     * @param args
     *            Command line arguments. Ignored.
     * @throws IOException
     *             On a failure closing the MongoClient.
     * @throws InterruptedException
     *             On a failure waiting for the demo to finish.
     */
    public static void main(final String[] args) throws IOException,
            InterruptedException {

        // Clear the collection.
        theCollection.deleteAsync(new DeleteCallback(), ALL);

        // See the stageX... methods below.

        // Wait for the demo to finish.
        doneLatch.await();

        // Always remember to close your client!
        client.close();
    }

    /**
     * Run the demo.
     * 
     * @param args
     *            Command line arguments. Ignored.
     * @throws IOException
     *             On a failure closing the MongoClient.
     * @throws InterruptedException
     *             On a failure waiting for the demo to finish.
     */
    public static void mainWithLambdas(final String[] args) throws IOException,
            InterruptedException {

        // Clear the collection.
        theCollection.deleteAsync((deleteThrown, deleteCount) -> {
            if (deleteThrown != null) {
                deleteThrown.printStackTrace();
                doneLatch.countDown();
                return;
            }

            // Insert 100 documents.
            final BatchedWrite.Builder write = BatchedWrite.builder();
            for (int i = 0; i < 100; ++i) {
                write.insert(d(e("name", "alice+" + i), e("interval", i),
                        e("time", new Date())));
            }

            theCollection.writeAsync((writeThrown, writeCount) -> {
                if (writeThrown != null) {
                    writeThrown.printStackTrace();
                    doneLatch.countDown();
                    return;
                }

                // Find all of the documents.
                theCollection.streamingFind((findThrown, document) -> {
                    // Error?
                    if (findThrown != null) {
                        findThrown.printStackTrace();
                        doneLatch.countDown();
                    }
                    // Document?
                    else if (document != null) {
                        count += 1;
                    }
                    // No error or document means the query is done.
                    else {
                        System.out.printf(
                                "Read %s documents.%n",
                                count);
                        doneLatch.countDown();
                    }
                }, ALL); // End Find/Query.
            }, write); // End Insert.
        } , ALL); // End Delete.

        // Wait for the demo to finish.
        doneLatch.await();

        // Always remember to close your client!
        client.close();
    }

    /**
     * Invoked when the delete is completed.
     * 
     * @param thrown
     *            The error in case the delete fails.
     * @param result
     *            The result of the delete.
     */
    public static void stage1DeleteDone(final Throwable thrown,
            final Long result) {

        if (thrown != null) {
            thrown.printStackTrace();
            doneLatch.countDown();
            return;
        }

        // Insert 100 documents.
        final BatchedWrite.Builder write = BatchedWrite.builder();
        for (int i = 0; i < 100; ++i) {
            write.insert(d(e("name", "alice+" + i), e("interval", i),
                    e("time", new Date())));
        }

        theCollection.writeAsync(new InsertCallback(), write);
    }

    /**
     * Invoked when the delete is completed.
     * 
     * @param thrown
     *            The error in case the insert fails.
     * @param result
     *            The result of the insert.
     */
    public static void stage2InsertDone(final Throwable thrown,
            final Long result) {
        if (thrown != null) {
            thrown.printStackTrace();
            doneLatch.countDown();
            return;
        }

        // Find all of the documents.
        theCollection.streamingFind(new FindCallback(), ALL);
    }

    /**
     * Invoked when a document is received or an error. If both thrown and
     * result are null then the stream has been exhausted.
     * 
     * @param thrown
     *            The error in case the find fails.
     * @param result
     *            The result of the delete.
     */
    public static void stage3Result(final Throwable thrown,
            final Document result) {
        // Error?
        if (thrown != null) {
            thrown.printStackTrace();
            doneLatch.countDown();
        }
        // Document?
        else if (result != null) {
            count += 1;
        }
        // No error or document. The query is done.
        else {
            System.out.printf("Read %s documents.%n", count);
            doneLatch.countDown();
        }
    }

    /**
     * DeleteCallback provides the callback for the initial delete of documents.
     * 
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class DeleteCallback implements LambdaCallback<Long> {
        @Override
        public void accept(final Throwable thrown, final Long result) {
            stage1DeleteDone(thrown, result);
        }
    }

    /**
     * StreamCallbackImplementation provides the callback for the streaming
     * find.
     * 
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class FindCallback implements
            LambdaCallback<Document> {
        @Override
        public void accept(final Throwable thrown, final Document result) {
            stage3Result(thrown, result);
        }
    }

    /**
     * InsertCallback provides the callback for the batched insert.
     * 
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class InsertCallback implements LambdaCallback<Long> {
        @Override
        public void accept(final Throwable thrown, final Long result) {
            stage2InsertDone(thrown, result);
        }
    }
}
