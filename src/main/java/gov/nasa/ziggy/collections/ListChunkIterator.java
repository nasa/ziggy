package gov.nasa.ziggy.collections;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Given a list, returns successive sub-lists which encompass all of the elements of the list. Each
 * sub-list is limited to some maximum size.
 *
 * @author Sean McCauliff
 */
public class ListChunkIterator<T> implements Iterator<List<T>>, Iterable<List<T>> {
    private final Iterator<T> source;
    private final int chunkSize;

    public ListChunkIterator(Iterator<T> source, int chunkSize) {
        if (source == null) {
            throw new NullPointerException("source");
        }
        this.source = source;
        this.chunkSize = chunkSize;
    }

    public ListChunkIterator(Iterable<T> source, int chunkSize) {
        this(source.iterator(), chunkSize);
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public List<T> next() {
        List<T> rv = new ArrayList<>(chunkSize);
        for (int i = 0; i < chunkSize && source.hasNext(); i++) {
            rv.add(source.next());
        }
        return rv;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    @Override
    public Iterator<List<T>> iterator() {
        return this;
    }
}
