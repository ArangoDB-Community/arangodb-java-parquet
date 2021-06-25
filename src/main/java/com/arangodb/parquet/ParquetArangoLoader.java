package com.arangodb.parquet;

import com.arangodb.ArangoCollection;
import com.arangodb.async.ArangoCollectionAsync;
import com.arangodb.parquet.serde.GenericRecordJsonEncoder;
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
    private static final int DEFAULT_BATCH_SIZE = 1000;

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
     *         Instant instant = (Instant) t;
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
        loadParquetFileIntoArango(parquetLocation, collection, false, DEFAULT_BATCH_SIZE);
    }

    public void loadParquetFileIntoArango(String parquetLocation, ArangoCollection collection, int batchSize) throws InvalidPathException, IOException {
        loadParquetFileIntoArango(parquetLocation, collection, false, batchSize);
    }

    public void loadParquetFileIntoArango(String parquetLocation, ArangoCollection collection, boolean overwriteCollection) throws InvalidPathException, IOException {
        loadParquetFileIntoArango(parquetLocation, collection, overwriteCollection, DEFAULT_BATCH_SIZE);
    }

    public void loadParquetFileIntoArango(String parquetLocation, ArangoCollection collection, boolean overwriteCollection, int batchSize) throws InvalidPathException, IOException {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize for document insertion must be at least 1.");
        }

        Path parquetPath = createPath(parquetLocation);

        if (!collection.exists()) {
            collection.create();
        }
        else if (overwriteCollection) {
            collection.drop();
            collection.create();
        }

        GenericRecordJsonEncoder encoder = this.createParquetToJsonEncoder();

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(parquetPath, new Configuration())).build();
        GenericRecord nextRecord;

        List<String> batch = new ArrayList<>(batchSize);
        while ((nextRecord = reader.read()) != null) {
            batch.add(encoder.serialize(nextRecord));

            if (batch.size() == batchSize) {
                collection.insertDocuments(batch);
                batch.clear();
            }
        }

        if (batch.size() > 0) {
            collection.insertDocuments(batch);
        }

        reader.close();
    }

    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        loadParquetFileIntoArangoAsync(parquetLocation, collection, false, DEFAULT_BATCH_SIZE);
    }

    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection, int batchSize) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        loadParquetFileIntoArangoAsync(parquetLocation, collection, false, batchSize);
    }

    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection, boolean overwriteCollection) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        loadParquetFileIntoArangoAsync(parquetLocation, collection, overwriteCollection, DEFAULT_BATCH_SIZE);
    }

    public void loadParquetFileIntoArangoAsync(String parquetLocation, ArangoCollectionAsync collection, boolean overwriteCollection, int batchSize) throws ExecutionException, InterruptedException, InvalidPathException, IOException {
        Path parquetPath = createPath(parquetLocation);

        if (!collection.exists().get()) {
            collection.create().get();
        }
        else if (overwriteCollection) {
            collection.drop().get();
            collection.create().get();
        }

        GenericRecordJsonEncoder encoder = this.createParquetToJsonEncoder();

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

    private GenericRecordJsonEncoder createParquetToJsonEncoder() {
        GenericRecordJsonEncoder encoder = new GenericRecordJsonEncoder();
        this.registerEncoderConversions(encoder);
        return encoder;
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
}
