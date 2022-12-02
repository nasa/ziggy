package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.BorderFactory;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.common.ZTable;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ViewParametersPanel extends javax.swing.JPanel {
    private ParameterSet parameterSet = null;
    private JScrollPane scrollPane;
    private ZTable parametersTable;
    private ParameterPropsTableModel parametersTableModel;

    public ViewParametersPanel(ParameterSet parameterSet) {
        this.parameterSet = parameterSet;

        initGUI();
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(400, 300));
            setBorder(BorderFactory.createTitledBorder("View Parameters"));
            this.add(getScrollPane(), BorderLayout.CENTER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getParametersTable());
        }
        return scrollPane;
    }

    private ZTable getParametersTable() {
        if (parametersTable == null) {
            parametersTableModel = new ParameterPropsTableModel(
                parameterSet.getParameters().getTypedProperties());
            parametersTable = new ZTable();
            parametersTable.setModel(parametersTableModel);
        }
        return parametersTable;
    }
}
