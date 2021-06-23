package com.arangodb;

import com.arangodb.async.ArangoCollectionAsync;
import com.arangodb.async.ArangoDBAsync;
import com.arangodb.async.ArangoDatabaseAsync;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.serde.GenericRecordJsonEncoder;
import com.sun.xml.internal.ws.util.CompletedFuture;
import org.apache.avro.LogicalType;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ParquetArangoLoader {
    private Map<LogicalType, Function<Object, Object>> converters;

    public ParquetArangoLoader() {
        converters = new HashMap<>();
    }

    /**
     * Create a Parquet Loader For ArangoDB
     * If there are non-standard types in your parquet files, you may have to specify a correct
     * type toString conversion here using Avro LogicalTypes. For example a millisecond timestamp:
     * <pre>{@code
     *     Map<LogicalType, Function<Object, Object>> converters = new HashMap<>();
     *     converters.put(LogicalTypes.timestampMillis(), t -> {
     *         Instant instant = (Instant)t;
     *         return DateTimeFormatter.ISO_INSTANT.format(instant);
     *     });
     * }</pre>
     *
     *
     * @param logicalTypeConverters
     */
    public ParquetArangoLoader(Map<LogicalType, Function<Object, Object>> logicalTypeConverters) {
        converters = logicalTypeConverters;
    }

    public void addTypeConverter(LogicalType type, Function<Object, Object> mapping) {
        converters.put(type, mapping);
    }

    public void loadParquetFileIntoArango(String parquetLocation, ArangoCollection collection) throws InvalidPathException, IOException {
        loadParquetFileIntoArango(parquetLocation, collection, false);
    }

    public void loadParquetFileIntoArango(String parquetLocation, ArangoCollection collection, boolean overwriteCollection) throws InvalidPathException, IOException {
        Path parquetPath = createPath(parquetLocation);

        if (!collection.exists()) {
            collection.create();
        }
        else if (overwriteCollection) {
            collection.drop();
            collection.create();
        }

        GenericRecordJsonEncoder encoder = new GenericRecordJsonEncoder();
        this.registerEncoderConversions(encoder);

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(parquetPath, new Configuration())).build();
        GenericRecord nextRecord;

        while ((nextRecord = reader.read()) != null) {
            String j = encoder.serialize(nextRecord);
            collection.insertDocument(j);
        }
        reader.close();
    }


    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection, boolean overwriteCollection) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        Path parquetPath = createPath(parquetLocation);

        if (!collection.exists().get()) {
            collection.create().get();
        }
        else if (overwriteCollection) {
            collection.drop().get();
            collection.create().get();
        }

        GenericRecordJsonEncoder encoder = new GenericRecordJsonEncoder();
        this.registerEncoderConversions(encoder);

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(parquetPath, new Configuration())).build();
        GenericRecord nextRecord;

        List<CompletableFuture> insertions = new ArrayList<>();
        while ((nextRecord = reader.read()) != null) {
            String j = encoder.serialize(nextRecord);
            insertions.add(collection.insertDocument(j));
        }
        reader.close();

        for (CompletableFuture f : insertions) {
            f.get();
        }
    }

    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        loadParquetFileIntoArangoAsync(parquetLocation, collection, false);
    }

    private void registerEncoderConversions(GenericRecordJsonEncoder encoder) {
        for (Map.Entry<LogicalType, Function<Object, Object>> entry: this.converters.entrySet()) {
            encoder.registerLogicalTypeConverter(entry.getKey(), entry.getValue());
        }
    }

    private Path createPath(String parquetFileString) throws InvalidPathException {
        // First check if the path is valid
        Paths.get(parquetFileString);

        return new Path(parquetFileString);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String fp = "/Users/alexgeenen/code/arangodb-java-parquet/src/main/resources/data.parquet";

        ArangoJack arangoJack = new ArangoJack();

        ArangoDB arangoDB = new ArangoDB.Builder().serializer(arangoJack).build();
        ArangoDatabase database = arangoDB.db("testDB");
        if (!database.exists()) {
            database.create();
        }

        ArangoCollection trafficCol = database.collection("trafficCol");

        ParquetArangoLoader parquetArangoLoader = new ParquetArangoLoader();
        try {
            parquetArangoLoader.loadParquetFileIntoArango(fp, trafficCol, true);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("Finished Encoding");

        arangoDB.shutdown();
    }
}
