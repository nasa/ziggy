package gov.nasa.ziggy.ui.util;

import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class GenericListModel<T> extends AbstractListModel<T> {
    private List<T> list = new LinkedList<>();

    public GenericListModel() {
    }

    public GenericListModel(List<T> list) {
        this.list = list;
    }

    @Override
    public int getSize() {
        return list.size();
    }

    @Override
    public T getElementAt(int index) {
        return list.get(index);
    }

    /**
     * @return Returns the list.
     */
    public List<T> getList() {
        return list;
    }

    public boolean add(T o) {
        boolean added = list.add(o);
        if (added) {
            fireIntervalAdded(this, list.size(), list.size());
        }
        return added;
    }

    public void clear() {
        int oldSize = list.size();
        list.clear();
        fireIntervalRemoved(this, 0, oldSize);
    }

    public void setList(List<T> newList) {
        clear();

        list = newList;
        fireIntervalAdded(this, 0, list.size());
    }

    public T get(int index) {
        return list.get(index);
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public T remove(int index) {
        T o = list.remove(index);
        fireIntervalRemoved(this, list.size(), list.size());
        return o;
    }
}
