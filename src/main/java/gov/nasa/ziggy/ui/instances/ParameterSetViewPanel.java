package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Display a {@link Map} from {@link ClassWrapper} instances to {@link ParameterSet} instances in
 * read-only mode. Used for viewing the parameters used for a particular pipeline instance.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetViewPanel extends javax.swing.JPanel {
    private ZiggyTable<ParameterSet> ziggyTable;

    public ParameterSetViewPanel(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
        buildComponent(parameterSetsMap);
    }

    private void buildComponent(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
        setLayout(new BorderLayout());

        add(createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("View", this::viewParameterSet)), BorderLayout.NORTH);
        add(createDataPanel(parameterSetsMap), BorderLayout.CENTER);
    }

    private JPanel createDataPanel(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {

        ziggyTable = new ZiggyTable<>(new ParameterSetTableModel(parameterSetsMap));

        JPanel growPanel = new JPanel(new BorderLayout());
        growPanel.add(new JScrollPane(ziggyTable.getTable()));

        return growPanel;
    }

    private void viewParameterSet(ActionEvent evt) {
        int selectedModelRow = ziggyTable.convertRowIndexToModel(ziggyTable.getSelectedRow());

        if (selectedModelRow == -1) {
            MessageUtil.showError(this, "No parameter set selected");
        } else {
            new ViewParametersDialog(SwingUtilities.getWindowAncestor(this),
                ziggyTable.getContentAtViewRow(selectedModelRow)).setVisible(true);
        }
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new ParameterSetViewPanel(null));
    }

    private static class ParameterSetTableModel extends AbstractZiggyTableModel<ParameterSet> {

        private static final String[] COLUMN_NAMES = { "Type", "Name" };

        protected List<ParameterSet> paramSets = new ArrayList<>();
        protected List<ClassWrapper<ParametersInterface>> paramSetTypes = new ArrayList<>();

        public ParameterSetTableModel(
            Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
            update(parameterSetsMap);
        }

        public void update(Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
            paramSets.clear();
            paramSetTypes.clear();

            if (parameterSetsMap != null) {
                for (ClassWrapper<ParametersInterface> classWrapper : parameterSetsMap.keySet()) {
                    ParameterSet paramSet = parameterSetsMap.get(classWrapper);

                    paramSets.add(paramSet);
                    paramSetTypes.add(classWrapper);
                }
            }

            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            return paramSets.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ParameterSet paramSet = paramSets.get(rowIndex);
            ClassWrapper<ParametersInterface> paramSetType = paramSetTypes.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> paramSetType.getClazz().getSimpleName();
                case 1 -> paramSet.getName();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<ParameterSet> tableModelContentClass() {
            return ParameterSet.class;
        }

        @Override
        public ParameterSet getContentAtRow(int row) {
            return paramSets.get(row);
        }
    }
}
