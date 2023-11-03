package gov.nasa.ziggy.metrics.report;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * This class manages an ordered, fixed-length list (list will contain up to but no more than N
 * items) of Comparables
 *
 * @author Todd Klaus
 */
public class TopNList implements Serializable {
    private static final long serialVersionUID = 20230511L;

    private int listMaxLength = 0;
    private final LinkedList<TopNListElement> list = new LinkedList<>();

    public TopNList(int listMaxLength) {
        this.listMaxLength = listMaxLength;
    }

    public void add(long value, String label) {
        TopNListElement element = new TopNListElement(value, label);

        if (list.isEmpty()) {
            list.add(element);
            return;
        }

        int currentIndex = list.size() - 1;
        boolean added = false;

        for (int i = currentIndex; i >= 0; i--) {
            if (element.getValue() < list.get(i).getValue()) {
                list.add(i + 1, element);
                added = true;
                break;
            }
        }

        if (!added) {
            // belongs at the top of the list
            list.add(0, element);
        }

        if (list.size() > listMaxLength) {
            list.removeLast();
        }
    }

    public List<TopNListElement> getList() {
        return list;
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
