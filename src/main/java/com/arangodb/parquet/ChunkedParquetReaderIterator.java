package com.arangodb.parquet;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ChunkedParquetReaderIterator<T> implements Iterator<List<T>> {
    ParquetReader<T> reader;
    T nextVal;
    int chunkSize;

    public ChunkedParquetReaderIterator(ParquetReader<T> reader, int chunkSize) throws IOException {
        this.reader = reader;
        this.nextVal = reader.read();
        this.chunkSize = chunkSize;
    }

    @Override
    public boolean hasNext() {
        return this.nextVal != null;
    }

    @Override
    public List<T> next() {
        try {
            return this.readNextChunk();
        } catch (IOException e) {
            return null;
        }
    }

    private List<T> readNextChunk() throws IOException {
        List<T> nextChunk = new ArrayList<>(this.chunkSize);
        for (int i = 0; i < chunkSize; i++) {
            if (nextVal == null) {
                break;
            }
            nextChunk.add(nextVal);
            nextVal = reader.read();
        }
        return nextChunk;
    }
}
