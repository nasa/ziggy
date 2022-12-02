package gov.nasa.ziggy.ui.config.parameters;

import java.util.LinkedList;
import java.util.List;

import javax.swing.ListModel;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.models.AbstractDatabaseListModel;
import gov.nasa.ziggy.ui.proxy.ParameterSetCrudProxy;

/**
 * {@link ListModel} that holds {@link ParameterSet}s of a specified type.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetListModel extends AbstractDatabaseListModel<ParameterSet> {
    Class<? extends Parameters> filterClass;

    List<ParameterSet> paramSets = new LinkedList<>();
    ParameterSetCrudProxy paramSetCrud = new ParameterSetCrudProxy();

    public ParameterSetListModel(Class<? extends Parameters> filterClass) {
        this.filterClass = filterClass;
    }

    @Override
    public void loadFromDatabase() {
        if (paramSets != null) {
            paramSetCrud.evictAll(paramSets);
        }

        paramSets = new LinkedList<>();

        List<ParameterSet> allParamSets = paramSetCrud.retrieveLatestVersions();

        for (ParameterSet parameterSet : allParamSets) {
            Class<?> clazz = null;

            try {
                clazz = parameterSet.getParameters().getClazz();
            } catch (RuntimeException e) {
                // ignore this parameter set
            }

            if (clazz != null && (filterClass == null || clazz.equals(filterClass))) {
                paramSets.add(parameterSet);
            }
        }

        fireContentsChanged(this, 0, paramSets.size() - 1);
    }

    @Override
    public ParameterSet getElementAt(int index) {
        validityCheck();
        return paramSets.get(index);
    }

    @Override
    public int getSize() {
        validityCheck();
        return paramSets.size();
    }
}
