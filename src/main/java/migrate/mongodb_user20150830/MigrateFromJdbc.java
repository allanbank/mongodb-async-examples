/*
 * #%L
 * MigrateFromJdbc.java - mongodb-async-examples - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2015 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package migrate.mongodb_user20150830;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Sort;

/**
 * Utility to migrate rows from a JDBC table into the MongoDB collection.
 *
 * @see <a href=
 *      "https://groups.google.com/d/msg/mongodb-user/iFzLQsxio7A/bafIHrpUBgAJ">
 *      mongodb-user question</a>
 *
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MigrateFromJdbc {

    /**
     * Runs the migration.
     *
     * @param args
     *            The command line args.
     */
    public static void main(final String[] args) {
        final MigrateFromJdbc migrate = new MigrateFromJdbc();
        try {
            if (migrate.parse(args)) {
                migrate.run();
            }
            else {
                migrate.usage();
            }
        }
        catch (final IllegalStateException | IOException | SQLException error) {
            System.out.println(error.getMessage());
            System.out.println();
            migrate.usage();
        }
        catch (final ClassNotFoundException cnfe) {
            System.out.println("Could not load class: " + migrate.myJdbcClass);
            System.out.println();
            migrate.usage();
        }
    }

    /** The JDBC Class. */
    private String myJdbcClass;

    /** The JDBC URI. */
    private String myJdbcUri;

    /** The MongoDB URI. */
    private String myMongoDbUri;

    /** The the name of the JDBC table. Used as the collection name too. */
    private String myTableName;

    /** The field used to determine if a document is "new". */
    private String myUpdateField;

    /**
     * Creates a new MigrateFromJdbc.
     */
    public MigrateFromJdbc() {
        super();
    }

    /**
     * Parses the arguments.
     *
     * @param args
     *            The command line arguments.
     * @return True if the arguments are parsed.
     *
     * @throws IllegalStateException
     *             If the arguments are incomplete.
     */
    public boolean parse(final String[] args) {

        if (args.length == 0) {
            return false;
        }

        for (final String arg : args) {
            if (arg.startsWith("--mongodb=")) {
                myMongoDbUri = arg.substring("--mongodb=".length());
            }
            else if (arg.startsWith("--jdbc=")) {
                myJdbcUri = arg.substring("--jdbc=".length());
            }
            else if (arg.startsWith("--jdbc-class=")) {
                myJdbcClass = arg.substring("--jdbc-class=".length());
            }
            else if (arg.startsWith("--table=")) {
                myTableName = arg.substring("--table=".length());
            }
            else if (arg.startsWith("--field=")) {
                myUpdateField = arg.substring("--field=".length());
            }
            else {
                throw new IllegalStateException("Unknown arguments: " + arg);
            }
        }

        if (myMongoDbUri == null) {
            throw new IllegalStateException("You must specify a MongoDB URI.");
        }
        if (myJdbcUri == null) {
            throw new IllegalStateException("You must specify a JDBC URI.");
        }
        if (myTableName == null) {
            throw new IllegalStateException(
                    "You must specify a JDBC table name.");
        }

        return true;
    }

    /**
     * Runs the migration.
     * 
     * @throws ClassNotFoundException
     *             On a failure to load the JDBC driver class.
     * @throws IOException
     *             On an I/O error.
     * @throws SQLException
     *             On a JDBC error.
     */
    public void run() throws ClassNotFoundException, IOException, SQLException {

        if (myJdbcClass != null) {
            Class.forName(myJdbcClass);
        }

        MongoDbUri mongoUri = new MongoDbUri(myMongoDbUri);
        try (MongoClient client = MongoFactory.createClient(mongoUri)) {
            MongoDatabase database = client.getDatabase(mongoUri.getDatabase());
            MongoCollection collection = database.getCollection(myTableName);
            BlockingQueue<Future<Integer>> pendingInsert = new ArrayBlockingQueue<>(
                    1024);

            Object from = null;
            if (myUpdateField != null) {
                // Use the _id (ObjectId) to find the last document added.
                Document latest = collection.findOne(Find.builder()
                        .projection(myUpdateField).sort(Sort.desc("_id")));
                if (latest != null) {
                    from = latest.get(myUpdateField).getValueAsObject();
                }
            }

            try (Connection conn = DriverManager.getConnection(myJdbcUri);
                    PreparedStatement statement = conn.prepareStatement(
                            (from == null) ? "SELECT * FROM " + myTableName
                                    : "SELECT * FROM " + myTableName + " where "
                                            + myUpdateField + " > ?")) {
                if (from != null) {
                    if (from instanceof Date) {
                        statement.setDate(1,
                                new java.sql.Date(((Date) from).getTime()));
                    }
                    else {
                        statement.setObject(1, from);
                    }
                }

                try (ResultSet rs = statement.executeQuery()) {
                    final ResultSetMetaData md = rs.getMetaData();
                    final int columnCount = md.getColumnCount();

                    final DocumentBuilder b = BuilderFactory.start();
                    while (rs.next()) {
                        b.reset();
                        for (int column = 1; column <= columnCount; ++column) {
                            b.add(md.getColumnName(column),
                                    map(md.getColumnType(column), column, rs));
                        }

                        Future<Integer> future = collection.insertAsync(b);
                        while (!pendingInsert.offer(future)) {
                            Future<Integer> oldest = pendingInsert.poll();
                            try {
                                if (oldest.get() != 1) {
                                    // Handle write error.
                                }
                            }
                            catch (InterruptedException
                                    | ExecutionException error) {
                                System.err.println(error.getMessage());
                            }
                        }
                    }
                }

                Future<Integer> oldest;
                while ((oldest = pendingInsert.poll()) != null) {
                    try {
                        if (oldest.get() != 1) {
                            // Handle write error.
                        }
                    }
                    catch (InterruptedException | ExecutionException error) {
                        System.err.println(error.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Prints a usage statement.
     */
    public void usage() {
        System.out.println("Usage: java " + MigrateFromJdbc.class.getName()
                + "--mongodb=uri --jdbc=uri --jdbc-class=driver-class --table=name [--field=name]");
    }

    /**
     * Maps the SQL column to a Java Type that MongoDB supports, if possible.
     * 
     * @param columnType
     *            The type for the column.
     * @param columnIndex
     *            The index of the column.
     * @param rs
     *            The result to extract from.
     * @return The mapped Java value.
     * @throws SQLException
     *             On a failure to map the column.
     */
    private Object map(int columnType, int columnIndex, ResultSet rs)
            throws SQLException {
        switch (columnType) {
        // Boolean
        case Types.BIT:
        case Types.BOOLEAN:
            return rs.getBoolean(columnIndex);

        // String
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
            return rs.getString(columnIndex);

        case Types.CLOB:
        case Types.NCLOB:
            throw new IllegalStateException(
                    "CLOB SQL Type not supported: " + columnType);

            // Null
        case Types.NULL:
            return null;

        // Double
        case Types.DECIMAL:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
            return rs.getDouble(columnIndex);

        // Integer/Long
        case Types.INTEGER:
        case Types.ROWID:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.BIGINT:
            return rs.getLong(columnIndex);

        // Date / Time
        case Types.DATE:
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE:
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
            return new Date(rs.getDate(columnIndex).getTime());

        // Bytes
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BINARY:
            return rs.getBytes(columnIndex);

        case Types.BLOB:
            throw new IllegalStateException(
                    "BLOB SQL Type not supported: " + columnType);

            // Unsupported types.
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.OTHER:
        case Types.REF:
        case Types.REF_CURSOR:
        case Types.SQLXML:
        case Types.STRUCT:
        case Types.ARRAY:
            throw new IllegalStateException(
                    "Unsupported SQL Type: " + columnType);
        }
        return null;
    }
}
