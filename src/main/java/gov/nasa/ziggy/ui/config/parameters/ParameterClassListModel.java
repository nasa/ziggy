package gov.nasa.ziggy.ui.config.parameters;

import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ListModel;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;

/**
 * {@link ListModel} that holds a cached list of all classes found on the classpath that implement
 * the {@link Parameters} interface
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterClassListModel extends AbstractListModel<ClassWrapper<Parameters>> {
    private List<ClassWrapper<Parameters>> list = null;

    public ParameterClassListModel(List<ClassWrapper<Parameters>> list) {
        this.list = list;
    }

    @Override
    public ClassWrapper<Parameters> getElementAt(int index) {
        return list.get(index);
    }

    @Override
    public int getSize() {
        return list.size();
    }

    public boolean add(ClassWrapper<Parameters> parameterClass) {
        boolean added = list.add(parameterClass);
        fireIntervalAdded(this, 0, list.size() - 1);
        return added;
    }

    public ClassWrapper<Parameters> remove(int index) {
        ClassWrapper<Parameters> removed = list.remove(index);
        fireIntervalRemoved(this, index, index);
        return removed;
    }

    public List<ClassWrapper<Parameters>> getParameterClassList() {
        return list;
    }
}
