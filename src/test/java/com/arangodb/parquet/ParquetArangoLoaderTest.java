package com.arangodb.parquet;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.async.ArangoCollectionAsync;
import com.arangodb.async.ArangoDBAsync;
import com.arangodb.async.ArangoDatabaseAsync;
import com.arangodb.mapping.ArangoJack;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class ParquetArangoLoaderTest {

    static final String TEST_DB = "PARQUET_ARANGO_TEST_DB";
    static final String TEST_COLLECTION = "TRAFFIC_COL";
    static ArangoDB arangoDB;
    static ArangoDatabase db;
    static ArangoDBAsync arangoDBAsync;
    static ArangoDatabaseAsync dbAsync;
    private String parquetFilePath;
    private long numDocs;

    public ParquetArangoLoaderTest(String filePath, long docs) {
        this.parquetFilePath = filePath;
        this.numDocs = docs;
    }

    static ArangoDB initConnection() {
        ArangoJack arangoJack = new ArangoJack();
        ArangoDB arangoDB = new ArangoDB.Builder().serializer(arangoJack).build();
        return arangoDB;
    }

    static ArangoDBAsync initConnectionAsync() {
        ArangoJack arangoJack = new ArangoJack();
        ArangoDBAsync arangoDB = new ArangoDBAsync.Builder().serializer(arangoJack).build();
        return arangoDB;
    }

    @BeforeClass
    public static void initDBs() throws ExecutionException, InterruptedException {
        if (arangoDB == null) {
            arangoDB = initConnection();
        }

        ArangoDatabase database = arangoDB.db(TEST_DB);
        if (database.exists()) {
            database.drop();
        }

        database.create();
        db = database;

        if (arangoDBAsync == null) {
            arangoDBAsync = initConnectionAsync();
        }

        ArangoDatabaseAsync databaseAsync = arangoDBAsync.db(TEST_DB);
        // This DB should already be initialized thanks to the connection above
        assert(databaseAsync.exists().get());

        dbAsync = databaseAsync;
    }

    @AfterClass
    public static void tearDown() {
        if (db != null && db.exists()) {
            db.drop();
        }
        if (arangoDB != null) {
            arangoDB.shutdown();
        }
        if (arangoDBAsync != null) {
            arangoDBAsync.shutdown();
        }
    }


    @Parameterized.Parameters
    public static Collection<Object[]> testFiles() {
        // Test resources along with the number of rows
        List<Object[]> resources = Arrays.asList(new Object[][]{
                {"testTypes.parquet", 26L},
                {"testTypesWithPDIndex.parquet", 26L},
                {"traffic.parquet", 2000L},
                {"trafficWithPDIndex.parquet", 2000L}
        });
        return resources.stream().map(i ->
            new Object[] {ParquetArangoLoaderTest.class.getClassLoader().getResource((String) i[0]).toString(), i[1]}
        ).collect(Collectors.toList());
    }

    @Test
    public void canLoadParquetSyncWithOverwrite() throws IOException {
        ArangoCollection col = db.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArango(this.parquetFilePath, col, true);
        assertThat(col.count().getCount(), is(numDocs));
    }

    @Test
    public void canLoadParquetSyncAsAppend() throws IOException {
        ArangoCollection col = db.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArango(this.parquetFilePath, col, true);
        loader.loadParquetFileIntoArango(this.parquetFilePath, col);
        assertThat(col.count().getCount(), is(numDocs*2));
    }

    @Test
    public void canLoadParquetSyncSmallerBatchSize() throws IOException {
        ArangoCollection col = db.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArango(this.parquetFilePath, col, true, 3);
        assertThat(col.count().getCount(), is(numDocs));
    }

    @Test
    public void canLoadParquetAsyncWithOverwrite() throws IOException, ExecutionException, InterruptedException {
        ArangoCollectionAsync col = dbAsync.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArangoAsync(this.parquetFilePath, col, true);
        assertThat(col.count().get().getCount(), is(this.numDocs));
    }

    @Test
    public void canLoadParquetAsyncAsAppend() throws IOException, ExecutionException, InterruptedException {
        ArangoCollectionAsync col = dbAsync.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArangoAsync(this.parquetFilePath, col, true);
        loader.loadParquetFileIntoArangoAsync(this.parquetFilePath, col);
        assertThat(col.count().get().getCount(), is(numDocs*2));
    }

    @Test
    public void canLoadParquetAsyncSmallerBatchSize() throws IOException, ExecutionException, InterruptedException {
        ArangoCollectionAsync col = dbAsync.collection(TEST_COLLECTION);

        ParquetArangoLoader loader = new ParquetArangoLoader();
        loader.loadParquetFileIntoArangoAsync(this.parquetFilePath, col, true, 3, ParquetArangoLoader.DEFAULT_MAX_PENDING_REQUESTS);
        assertThat(col.count().get().getCount(), is(this.numDocs));
    }
}