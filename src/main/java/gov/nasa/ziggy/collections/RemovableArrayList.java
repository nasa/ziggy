package gov.nasa.ziggy.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Allows for slightly more efficent means of removing and insterting intervals of data from an
 * ArrayList.
 * <p>
 * Exposes the method which removes a range of elements from the ArrayList which for some reason is
 * protected in java.util.ArrayList.
 *
 * @author Sean McCauliff
 */
public class RemovableArrayList<T> extends ArrayList<T> {
    private static final long serialVersionUID = -7536249496170417383L;

    public RemovableArrayList() {
    }

    /**
     * @param initialCapacity
     */
    public RemovableArrayList(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * @param c
     */
    public RemovableArrayList(Collection<T> c) {
        super(c);
    }

    /**
     * Removes a section of the array. ArrayList implements this method as removeRange, but does not
     * expose it as public.
     *
     * @param start
     * @param end Exclusive
     */
    public void removeInterval(int start, int end) {
        super.removeRange(start, end);
    }

    /**
     * Inserts data at the speciried location.
     */
    public void insertAt(int start, final T[] data) {
        Collection<T> tmpCollection = new Collection<T>() {
            @Override
            public boolean add(T e) {
                throw new IllegalStateException("Read only.");
            }

            @Override
            public boolean addAll(Collection<? extends T> c) {
                throw new IllegalStateException("Read only.");
            }

            @Override
            public void clear() {
                throw new IllegalStateException("Read only.");
            }

            @Override
            public boolean contains(Object o) {
                throw new IllegalStateException("Not implemented.");
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new IllegalStateException("Not implemented.");
            }

            @Override
            public boolean isEmpty() {
                return data.length == 0;
            }

            @Override
            public Iterator<T> iterator() {
                throw new IllegalStateException("Not implemented.");
            }

            @Override
            public boolean remove(Object o) {
                throw new IllegalStateException("Read only.");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new IllegalStateException("Read only.");
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new IllegalStateException("Not implemented.");
            }

            @Override
            public int size() {
                return data.length;
            }

            @Override
            public Object[] toArray() {
                return data;
            }

            @Override
            public <TRv> TRv[] toArray(TRv[] a) {
                throw new IllegalStateException("Not implemented.");
            }
        };

        super.addAll(start, tmpCollection);
    }
}
