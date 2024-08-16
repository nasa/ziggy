package gov.nasa.ziggy.ui.pipeline;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.JList;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;

/**
 * Select a {@link ParameterSet} from a list or create a new one.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetSelectorPanel extends javax.swing.JPanel {
    private JList<ParameterSet> paramSetList;
    private ParameterSetListModel paramSetListModel;

    public ParameterSetSelectorPanel() {
        buildComponent();
    }

    private void buildComponent() {
        setLayout(new BorderLayout());
        setPreferredSize(new Dimension(400, 300));

        paramSetListModel = new ParameterSetListModel();
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

    private static class ParameterSetListModel extends AbstractListModel<ParameterSet>
        implements DatabaseModel {

        private List<ParameterSet> paramSets = new LinkedList<>();

        private final ParametersOperations parametersOperations = new ParametersOperations();

        public ParameterSetListModel() {
            loadFromDatabase();
        }

        @Override
        public void loadFromDatabase() {
            paramSets = parametersOperations().parameterSets();
            fireContentsChanged(this, 0, paramSets.size() - 1);
        }

        @Override
        public ParameterSet getElementAt(int index) {
            return paramSets.get(index);
        }

        @Override
        public int getSize() {
            return paramSets.size();
        }

        private ParametersOperations parametersOperations() {
            return parametersOperations;
        }
    }
}
