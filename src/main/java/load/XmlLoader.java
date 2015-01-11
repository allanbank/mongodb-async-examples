/*
 *           Copyright 2015 - Allanbank Consulting, Inc.
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
package load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.util.IOUtils;

/**
 * A sample utility program to load XML files into MongoDB.
 * <p>
 * This utility can operate in one of two modes:
 * <ul>
 * <li><b>--files</b> assumes that each XML document is a distinct file in the
 * directory/files provided on the command line.</li>
 * <li><b>--lines</b> assumes that each XML document is a distinct line in the
 * directory/files provided on the command line.</li>
 * </ul>
 * </p>
 * <p>
 * Each XML document is parsed into a DOM object that is then converted into a
 * MongoDB BSON document. The BSON document is a straight forward mapping of the
 * XML attributes and sub-nodes. If the sub-node contains only a single
 * {@link Text} elements then it is converted to a field otherwise it is
 * converted to a sub-document. Multiple non-whitespace {@link Text} elements
 * are added to a single array named "_text" in the parent document.
 * </p>
 * <p>
 * This class was inspired by a <a
 * href="https://groups.google.com/forum/#!topic/mongodb-user/L7SrInnYTus"
 * >MongoDB User's Group Question</a>.
 * </p>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */

public class XmlLoader {

    /**
     * Run the XML loader.
     *
     * @param args
     *            the command line arguments.
     */
    public static void main(final String[] args) {
        final XmlLoader loader = new XmlLoader();
        if (loader.parseArgs(args) && loader.run()) {
            System.exit(0);
        }
        System.exit(-1);
    }

    /** The MongoDB client connected to the MongoDB server. */
    private MongoClient myClient;

    /** The MongoDB collection to add the documents to. */
    private MongoCollection myCollection;

    /** The factory for the XML parsers. */
    private final DocumentBuilderFactory myDocumentBuilderFactory;

    /** If true then each XML document is a single line in the file. */
    private Boolean myParseLine;

    /** The futures for the pending inserts. */
    private BlockingQueue<Future<Integer>> myPendingInserts;

    /** The files/directories provided on the command line that we should load. */
    private final List<File> myToLoad;

    /** The queue of files to be processed. */
    private final BlockingQueue<File> myToProcess;

    /** The MongoDB URL to use when connecting to MongoDB. */
    private String myUrl;

    /**
     * Creates a new XmlLoader.
     */
    public XmlLoader() {
        myParseLine = null;
        myPendingInserts = null;

        myUrl = "mongodb://localhost:27017/db.test";

        myToLoad = new ArrayList<File>();
        myToProcess = new LinkedBlockingQueue<File>();

        myDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
    }

    /**
     * Parses the command line arguments.
     *
     * @param args
     *            The command line arguments.
     * @return True if the parse was successful.
     */
    public boolean parseArgs(final String[] args) {
        for (int i = 0; i < args.length; ++i) {

            final String arg = args[i];
            if ("--files".equalsIgnoreCase(arg)) {
                i += 1;
                if (i < args.length) {
                    myToLoad.add(new File(args[i]));
                }
                else {
                    return error("Unmatched " + arg + " argument.");
                }

                if (myParseLine == null) {
                    myParseLine = Boolean.FALSE;
                }
                else if (myParseLine.equals(Boolean.TRUE)) {
                    return error("Cannot mix parsing --lines and --files.");
                }
            }
            else if ("--lines".equalsIgnoreCase(arg)) {
                i += 1;
                if (i < args.length) {
                    myToLoad.add(new File(args[i]));
                }
                else {
                    return error("Unmatched --lines argument.");
                }

                if (myParseLine == null) {
                    myParseLine = Boolean.TRUE;
                }
                else if (myParseLine.equals(Boolean.FALSE)) {
                    return error("Cannot mix parsing --lines and --files.");
                }
            }
            else if ("--url".equalsIgnoreCase(arg)) {
                i += 1;
                if (i < args.length) {
                    myUrl = args[i];
                }
                else {
                    return error("Unmatched --url argument.");
                }
            }
        }

        if (myToLoad.isEmpty()) {
            return error("Must supply at least 1 file or directory to load.");
        }

        return true;
    }

    /**
     * Loads each XML document.
     *
     * @return True if the run completed without errors.
     */
    public boolean run() {
        final MongoDbUri uri = new MongoDbUri(myUrl);

        myClient = MongoFactory.createClient(uri);

        final String dbCollection = uri.getDatabase();
        final int dotIndex = dbCollection.indexOf('.');
        if (dotIndex > 0) {
            myCollection = myClient.getDatabase(
                    dbCollection.substring(0, dotIndex)).getCollection(
                    dbCollection.substring(dotIndex + 1));
        }
        else {
            return error("You must specify the database and collection in the MongoDB URI.");
        }

        // Seed the set of files to process.
        myToProcess.addAll(myToLoad);

        final MongoClientConfiguration config = myClient.getConfig();
        final int threads = config.getMaxConnectionCount();

        myPendingInserts = new ArrayBlockingQueue<Future<Integer>>(Math.min(
                4096, threads * 1000));

        final Loader[] loaders = new Loader[threads];
        final Thread[] loadThreads = new Thread[threads];
        for (int i = 0; i < loaders.length; ++i) {
            loaders[i] = new Loader();
            loadThreads[i] = new Thread(loaders[i], "Load Thread - " + i);

            loadThreads[i].start();
        }

        for (int i = 0; i < loaders.length; ++i) {
            try {
                loadThreads[i].join();
            }
            catch (final InterruptedException e) {
                return error("Interrupted while waiting for the load threads: "
                        + e.getMessage());
            }
        }

        boolean success = true;
        for (final Loader loader : loaders) {
            final Throwable error = loader.getError();

            if (error != null) {
                success = false;
                System.err.println(error.getMessage());
            }
        }
        if (!success) {
            printUsage();
        }
        return success;
    }

    /**
     * Coerces the name/value into an element.
     *
     * @param nodeName
     *            The name for the element.
     * @param nodeValue
     *            The value for the element.
     * @return The element.
     */
    protected Element coerce(final String nodeName, final String nodeValue) {
        return new StringElement(nodeName, nodeValue);
    }

    /**
     * Converts the XML {@link Document} into a BSON {@link DocumentAssignable
     * document}.
     *
     * @param xmlDocument
     *            The SML document.
     * @return The BSON document.
     */
    protected DocumentAssignable convert(final Document xmlDocument) {
        final DocumentBuilder builder = BuilderFactory.start();

        convert(builder, xmlDocument.getDocumentElement());

        return builder;
    }

    /**
     * Processes all of the files in the {@link #myToProcess} queue until the
     * queue is empty. This method is run by multiple threads.
     *
     * @throws InterruptedException
     *             If the thread is interrupted.
     * @throws IOException
     *             On a failure reading a file.
     * @throws ParserConfigurationException
     *             On a failure to create an XML parser.
     * @throws SAXException
     *             On a failure to parse the XML document.
     * @throws ExecutionException
     *             On a failure to insert a document.
     */
    protected void doLoad() throws IOException, InterruptedException,
            ParserConfigurationException, SAXException, ExecutionException {
        File file = myToProcess.poll();
        while (file != null) {

            if (file.isDirectory()) {
                for (final String name : file.list()) {
                    myToProcess.put(new File(file, name));
                }
            }
            else if (file.isFile()) {
                if (myParseLine) {
                    loadLines(file);
                }
                else {
                    loadFile(file);
                }
            }
            else {
                System.err.println("Cannot read '" + file + "'.");
            }

            file = myToProcess.poll();
        }

        // Read the rest of the results.
        Future<Integer> pend = null;
        while ((pend = myPendingInserts.poll()) != null) {
            pend.get();
        }
    }

    /**
     * Loads the single file into the MongoDB database.
     *
     * @param file
     *            The file to be loaded.
     * @throws IOException
     *             On a failure reading the file.
     * @throws ParserConfigurationException
     *             On a failure to create an XML parser.
     * @throws SAXException
     *             On a failure to parse the XML document.
     * @throws ExecutionException
     *             On a failure to insert a document.
     * @throws InterruptedException
     *             On a failure to wait for the results on the insert.
     */
    protected void loadFile(final File file) throws IOException,
            ParserConfigurationException, SAXException, InterruptedException,
            ExecutionException {

        // Using factory get an instance of document builder
        final javax.xml.parsers.DocumentBuilder db = myDocumentBuilderFactory
                .newDocumentBuilder();
        final Document xmlDocument = db.parse(file);

        final Future<Integer> future = myCollection
                .insertAsync(convert(xmlDocument));
        while (!myPendingInserts.offer(future)) {
            final Future<Integer> pend = myPendingInserts.poll();
            if (pend != null) {
                pend.get();
            }
        }
    }

    /**
     * Loads the single file into the MongoDB database.
     *
     * @param file
     *            The file to be loaded.
     * @throws IOException
     *             On a failure reading the file.
     * @throws ParserConfigurationException
     *             On a failure to create an XML parser.
     * @throws SAXException
     *             On a failure to parse the XML document.
     * @throws ExecutionException
     *             On a failure to insert a document.
     * @throws InterruptedException
     *             On a failure to wait for the results on the insert.
     */
    protected void loadLines(final File file) throws IOException,
            ParserConfigurationException, SAXException, InterruptedException,
            ExecutionException {

        FileReader reader = null;
        BufferedReader bReader = null;
        try {
            reader = new FileReader(file);
            bReader = new BufferedReader(reader);

            // Using factory get an instance of document builder
            final javax.xml.parsers.DocumentBuilder db = myDocumentBuilderFactory
                    .newDocumentBuilder();

            String line = null;
            while ((line = bReader.readLine()) != null) {
                final Document xmlDocument = db.parse(new InputSource(
                        new StringReader(line)));

                final Future<Integer> future = myCollection
                        .insertAsync(convert(xmlDocument));

                while (!myPendingInserts.offer(future)) {
                    final Future<Integer> pend = myPendingInserts.poll();
                    if (pend != null) {
                        pend.get();
                    }
                }
            }
        }
        finally {
            IOUtils.close(bReader);
            IOUtils.close(reader);
        }
    }

    /**
     * Prints a usage statement.
     */
    protected void printUsage() {
        System.err
                .println("Usage: java "
                        + XmlLoader.class.getName()
                        + " [--url <mongodb_uri>]"
                        + " ( [--files <file|directory>]+ | [--lines <file|directory>]+ ) ");

        System.err.println();
        System.err
                .println("  --url <mongodb_uri>        : The URL for the MongoDB server.");
        System.err
                .println("                               Defaults to mongodb://localhost:27017/db.test");
        System.err
                .println("                               This adds the documents to the 'test' collection in the 'db' database.");
        System.err.println();
        System.err
                .println("  --files <file|directory>]+ : The file or directory of the files to parse.");
        System.err
                .println("                               One XML document per file.");
        System.err.println();
        System.err
                .println("  --lines <file|directory>]+ : The file or directory of the files to parse.");
        System.err
                .println("                               One XML document per line in each file.");
        System.err.println();
        System.err.println("Note: You cannot mix --lines and --files.");
    }

    /**
     * Make sure the name is a valid MongoDB document field name.
     *
     * @param name
     *            The name to cleanup.
     * @return The cleaned name.
     */
    private String cleanName(final String name) {
        String result = name;
        if (name.startsWith("$")) {
            result = "_" + name;
        }

        return result.replace('.', '_');
    }

    /**
     * Appends the {@link Node}'s attributes and children to the document.
     *
     * @param builder
     *            The builder for the document.
     * @param xmlNode
     *            The XML node to convert.
     */
    private void convert(final DocumentBuilder builder, final Node xmlNode) {
        final NamedNodeMap attributes = xmlNode.getAttributes();
        final NodeList children = xmlNode.getChildNodes();

        Map<String, Integer> nameCount = new HashMap<String, Integer>();
        Map<String, ArrayBuilder> duplicates = new HashMap<String, ArrayBuilder>();

        // Two passes through the structures.

        // First time we just count names.
        if (attributes != null) {
            for (int i = 0; i < attributes.getLength(); ++i) {
                final Node attr = attributes.item(i);
                final String name = cleanName(attr.getNodeName());

                if (nameCount.containsKey(name)) {
                    nameCount.put(name, nameCount.get(name) + 1);
                }
                else {
                    nameCount.put(name, 1);
                }
            }
        }
        for (int i = 0; i < children.getLength(); ++i) {
            final Node child = children.item(i);

            if (child instanceof Text) {
                // skip.
            }
            else {
                final String name = cleanName(child.getNodeName());
                if (nameCount.containsKey(name)) {
                    nameCount.put(name, nameCount.get(name) + 1);
                }
                else {
                    nameCount.put(name, 1);
                }
            }
        }

        // Second pass we build the documents.
        if (attributes != null) {
            for (int i = 0; i < attributes.getLength(); ++i) {
                final Node attr = attributes.item(i);
                final String name = cleanName(attr.getNodeName());

                if (nameCount.containsKey(name) && nameCount.get(name) > 1) {
                    ArrayBuilder arrayBuilder = duplicates.get(name);
                    if (arrayBuilder == null) {
                        arrayBuilder = builder.pushArray(name);
                        duplicates.put(name, arrayBuilder);
                    }

                    arrayBuilder.add(coerce(name, attr.getNodeValue()));
                }
                else {
                    builder.add(coerce(name, attr.getNodeValue()));
                }
            }
        }

        ArrayBuilder textSegments = null;
        for (int i = 0; i < children.getLength(); ++i) {
            final Node child = children.item(i);

            if (child instanceof Text) {
                final Text textNode = (Text) child;
                final String text = textNode.getData().trim();

                // Skip whitespace.
                if (text.length() > 0) {
                    if (textSegments == null) {
                        textSegments = builder.pushArray("_text");
                    }
                    textSegments.add(text);
                }
            }
            else {
                final String name = cleanName(child.getNodeName());
                if (textNode(child)) {
                    if (nameCount.containsKey(name) && nameCount.get(name) > 1) {
                        ArrayBuilder arrayBuilder = duplicates.get(name);
                        if (arrayBuilder == null) {
                            arrayBuilder = builder.pushArray(name);
                            duplicates.put(name, arrayBuilder);
                        }

                        arrayBuilder.add(coerce(name, convertToText(child)));
                    }
                    else {
                        builder.add(coerce(name, convertToText(child)));
                    }
                }
                else {
                    if (nameCount.containsKey(name) && nameCount.get(name) > 1) {
                        ArrayBuilder arrayBuilder = duplicates.get(name);
                        if (arrayBuilder == null) {
                            arrayBuilder = builder.pushArray(name);
                            duplicates.put(name, arrayBuilder);
                        }

                        convert(arrayBuilder.push(), child);
                    }
                    else {
                        convert(builder.push(name), child);
                    }
                }
            }
        }
    }

    /**
     * Converts the presumed {@link #textNode(Node)} into a string value.
     *
     * @param xmlNode
     *            The node to inspect.
     * @return The value of the node.
     */
    private String convertToText(final Node xmlNode) {
        final NodeList children = xmlNode.getChildNodes();
        if ((children.getLength() == 1) && (children.item(0) instanceof Text)) {
            return ((Text) children.item(0)).getData().trim();
        }
        return "";
    }

    /**
     * Reports the error and prints a usage statement.
     *
     * @param message
     *            The error message.
     * @return False to indicate an error.
     */
    private boolean error(final String message) {
        System.err.println(message);
        System.err.println();
        printUsage();

        return false;
    }

    /**
     * Determines if the xmlNode has no attributes and only a single child text
     * node.
     *
     * @param xmlNode
     *            The node to inspect.
     * @return True if xmlNode has no attributes and only a single child text
     *         node, false otherwise.
     */
    private boolean textNode(final Node xmlNode) {
        final NamedNodeMap attributes = xmlNode.getAttributes();
        final NodeList children = xmlNode.getChildNodes();

        return ((attributes == null) || (attributes.getLength() == 0))
                && (children.getLength() == 1)
                && (children.item(0) instanceof Text);
    }

    /**
     * Runnable to load documents.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class Loader
            implements Runnable {

        /** Any error encountered. */
        private Throwable myError = null;

        /**
         * Returns the exception thrown, if any.
         *
         * @return Returns the exception thrown, if any.
         */
        public Throwable getError() {
            return myError;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to load documents.
         * </p>
         *
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            try {
                doLoad();
            }
            catch (InterruptedException | IOException
                    | ParserConfigurationException | SAXException
                    | ExecutionException | RuntimeException error) {
                myError = error;
            }
        }

    }
}
