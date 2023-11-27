package gov.nasa.ziggy.ui.pipeline;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JList;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseListModel;
import gov.nasa.ziggy.ui.util.proxy.ParameterSetCrudProxy;

/**
 * Select a {@link ParameterSet} from a list or create a new one.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetSelectorPanel extends javax.swing.JPanel {
    private JList<ParameterSet> paramSetList;
    private ParameterSetListModel paramSetListModel;

    private final Class<? extends ParametersInterface> filterClass;

    public ParameterSetSelectorPanel() {
        filterClass = null;
        buildComponent();
    }

    public ParameterSetSelectorPanel(Class<? extends ParametersInterface> filterClass) {
        this.filterClass = filterClass;
        buildComponent();
    }

    private void buildComponent() {
        setLayout(new BorderLayout());
        setPreferredSize(new Dimension(400, 300));

        paramSetListModel = new ParameterSetListModel(filterClass);
        paramSetList = new JList<>(paramSetListModel);
        add(new JScrollPane(paramSetList), BorderLayout.CENTER);
    }

    public ParameterSet getSelected() {
        int selectedIndex = paramSetList.getSelectedIndex();

        if (selectedIndex != -1) {
            return paramSetListModel.getElementAt(selectedIndex);
        }
        return null;
    }

    private static class ParameterSetListModel extends AbstractDatabaseListModel<ParameterSet> {
        Class<? extends ParametersInterface> filterClass;

        List<ParameterSet> paramSets = new LinkedList<>();
        ParameterSetCrudProxy paramSetCrud = new ParameterSetCrudProxy();

        public ParameterSetListModel(Class<? extends ParametersInterface> filterClass) {
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
                    clazz = parameterSet.clazz();
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
}
