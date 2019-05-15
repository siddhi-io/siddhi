package io.siddhi.core.query.table.util;

import io.siddhi.core.table.record.RecordIterator;

import java.io.IOException;
import java.util.Iterator;

public class TestStoreWithCacheIterator implements RecordIterator<Object[]> {

    private Iterator<Object[]> iterator;

    public TestStoreWithCacheIterator(Iterator<Object[]> iterator) {
        this.iterator = iterator;
    }


    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Object[] next() {
        return iterator.next();
    }
}
