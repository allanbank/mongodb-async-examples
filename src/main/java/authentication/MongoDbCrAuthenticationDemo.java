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

package authentication;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.IOException;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;

/**
 * PlainAuthenticationDemo provides a simple example of authenticating using the
 * plain SASL for LDAP authentication. It can also be used for PAM
 * authentication.
 * 
 * @see <a
 *      href="http://docs.mongodb.org/master/tutorial/configure-ldap-sasl-authentication/">Configure
 *      LDAP SASL Authentication</a>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbCrAuthenticationDemo {
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

        // Update the configuration to authenticate using MongoDB user/name
        // passwords based challenge/response.
        final Credential.Builder credential = Credential.builder();

        credential.userName("testuser");
        credential.password("testpassword".toCharArray());
        credential.mongodbCR();

        client.getConfig().addCredential(credential);

        // Write?
        final ObjectId id = new ObjectId();
        theCollection.insert(BuilderFactory.start().add("_id", id));

        // Read?
        for (final Document doc : theCollection.find(Find.ALL)) {
            System.out.println(doc);
        }

        // Delete?
        theCollection.delete(where("_id").equals(id));

        // Always remember to close your client!
        client.close();
    }
}
