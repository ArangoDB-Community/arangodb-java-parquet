package com.arangodb.parquet;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ChunkedParquetReaderIterator<T> implements Iterator<List<T>> {
    ParquetReader<T> reader;
    ArrayList<T> nextChunk;
    T nextVal;
    int chunkSize;

    public ChunkedParquetReaderIterator(ParquetReader<T> reader, int chunkSize) throws IOException {
        this.reader = reader;
        this.nextVal = reader.read();
        this.chunkSize = chunkSize;
        this.nextChunk = new ArrayList<>(chunkSize);
        this.readNextChunk();
    }

    @Override
    public boolean hasNext() {
        return this.nextChunk.size() > 0;
    }

    @Override
    public List<T> next() {
        List<T> toReturn = (List) nextChunk.clone();
        try {
            this.readNextChunk();
            return toReturn;
        } catch (IOException e) {
            return null;
        }
    }

    private void readNextChunk() throws IOException {
        nextChunk.clear();
        for (int i = 0; i < chunkSize; i++) {
            if (nextVal == null) {
                break;
            }
            nextChunk.add(nextVal);
            nextVal = reader.read();
        }
    }
}
