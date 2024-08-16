package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Dialog for viewing (read-only) a {@link ParameterSet} object.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ViewParametersDialog extends javax.swing.JDialog {

    public ViewParametersDialog(Window owner, ParameterSet parameterSet) {
        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(parameterSet);
        setLocationRelativeTo(owner);
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private void buildComponent(ParameterSet parameterSet) {
        try {
            setTitle("View parameter set");

            getContentPane().add(createDataPanel(parameterSet), BorderLayout.CENTER);
            getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
                BorderLayout.SOUTH);
            setPreferredSize(ZiggyGuiConstants.MIN_DIALOG_SIZE);

            pack();
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    private JPanel createDataPanel(ParameterSet parameterSet) {
        JLabel name = boldLabel("Name");
        JLabel nameTextField = new JLabel(parameterSet.getName());

        JLabel parameters = boldLabel("Parameters");
        ZiggyTable<ParameterProperties> ziggyTable = new ZiggyTable<>(
            new ParameterPropsTableModel(parameterSet.getParameters()));
        JScrollPane parametersScrollPane = new JScrollPane(ziggyTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameTextField)
            .addComponent(parameters)
            .addComponent(parametersScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameTextField)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(parameters)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(parametersScrollPane));

        return dataPanel;
    }

    private static class ParameterPropsTableModel
        extends AbstractZiggyTableModel<ParameterProperties> {

        private static final String[] COLUMN_NAMES = { "Name", "Value" };

        private final List<ParameterProperties> parameterProperties;

        public ParameterPropsTableModel(Set<Parameter> properties) {
            parameterProperties = new ArrayList<>(properties.size());

            for (Parameter property : properties) {
                parameterProperties.add(new ParameterProperties(property));
            }
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public int getRowCount() {
            return parameterProperties.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return switch (columnIndex) {
                case 0 -> parameterProperties.get(rowIndex).getName();
                case 1 -> parameterProperties.get(rowIndex).getValue();
                default -> "Huh?";
            };
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<ParameterProperties> tableModelContentClass() {
            return ParameterProperties.class;
        }

        @Override
        public ParameterProperties getContentAtRow(int row) {
            return parameterProperties.get(row);
        }
    }

    private static class ParameterProperties {

        private final String name;
        private final String value;

        public ParameterProperties(Parameter parameter) {
            name = parameter.getName();
            value = parameter.getString();
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }
    }
}
