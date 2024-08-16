package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Display a {@link Set} of {@link ParameterSet} instances in read-only mode. Used for viewing the
 * parameters used for a particular pipeline instance.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 * @author PT
 */
@SuppressWarnings("serial")
public class ParameterSetViewPanel extends javax.swing.JPanel {
    private ZiggyTable<ParameterSet> ziggyTable;

    public ParameterSetViewPanel(Set<ParameterSet> parameterSets) {
        buildComponent(parameterSets);
    }

    private void buildComponent(Set<ParameterSet> parameterSets) {
        setLayout(new BorderLayout());

        add(createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("View", this::viewParameterSet)), BorderLayout.NORTH);
        add(createDataPanel(parameterSets), BorderLayout.CENTER);
    }

    private JPanel createDataPanel(Set<ParameterSet> parameterSets) {

        ziggyTable = new ZiggyTable<>(new ParameterSetTableModel(parameterSets));

        JPanel growPanel = new JPanel(new BorderLayout());
        growPanel.add(new JScrollPane(ziggyTable.getTable()));

        return growPanel;
    }

    private void viewParameterSet(ActionEvent evt) {
        int selectedModelRow = ziggyTable.convertRowIndexToModel(ziggyTable.getSelectedRow());

        if (selectedModelRow == -1) {
            MessageUtils.showError(this, "No parameter set selected");
        } else {
            new ViewParametersDialog(SwingUtilities.getWindowAncestor(this),
                ziggyTable.getContentAtViewRow(selectedModelRow)).setVisible(true);
        }
    }

    public static void main(String[] args) {
        ZiggySwingUtils
            .displayTestDialog(new ParameterSetViewPanel(Set.of(new ParameterSet("foo"))));
    }

    private static class ParameterSetTableModel extends AbstractZiggyTableModel<ParameterSet> {

        private static final String[] COLUMN_NAMES = { "Name" };

        protected List<ParameterSet> paramSets = new ArrayList<>();

        public ParameterSetTableModel(Set<ParameterSet> parameterSets) {
            update(parameterSets);
        }

        public void update(Set<ParameterSet> parameterSets) {
            paramSets.clear();
            paramSets.addAll(parameterSets);
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

            return switch (columnIndex) {
                case 0 -> paramSet.getName();
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
