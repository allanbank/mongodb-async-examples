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

import javax.net.ssl.SSLSocketFactory;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;

/**
 * TlsDemo provides a simple example of authenticating using the x.509
 * certificates including setting up a secure TLS connection.
 * <p>
 * x.509 authentication requires MongoDB Enterprise and the driver's extensions.
 * </p>
 * 
 * @see <a
 *      href="http://docs.mongodb.org/manual/tutorial/configure-x509/">Authenticate
 *      with x.509 Certificate</a>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TlsDemo {
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

        // Use the vanilla SSL Socket factory for a basic SSL connection
        // that is vulnerable to man-in-middle attacks.
        client.getConfig().setSocketFactory(SSLSocketFactory.getDefault());

        /**
         * The extensions have a socket factory builder with host name
         * verification built in.
         * 
         * <pre>
         * <code>
         * final File keyStore = new File("keystore");
         * final File trustStore = new File("trust");
         * 
         * final TlsSocketFactory.Builder socketFactoryBuilder = TlsSocketFactory
         *         .builder()
         *         .ciphers(CipherName.AES_CIPHERS)
         *         .hostnameVerifier(
         *                 HttpsURLConnection.getDefaultHostnameVerifier())
         *         .keys(keyStore, "JKS", "changeme".toCharArray(),
         *                 "changeit".toCharArray())
         *         .trustOnly(trustStore, "JKS", "changeme".toCharArray());
         * 
         * client.getConfig().setSocketFactory(socketFactoryBuilder.build());
         * </code>
         * </pre>
         */
        // Update the configuration to authenticate using X.509 client
        // certificates.
        final Credential.Builder credential = Credential.builder()
                .userName("CN=testuser").x509();

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
