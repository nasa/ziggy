package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;

/**
 * Display a {@link Map} from {@link ClassWrapper} instances to {@link ParameterSet} instances in
 * read-only mode. Used for viewing the parameters used for a particular pipeline instance.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetViewPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetViewPanel.class);

    private JScrollPane paramSetsScrollPane;
    private JButton viewParamSetButton;
    private JPanel buttonPanel;
    private ZTable paramSetsTable;
    private Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap = new HashMap<>();
    private ParameterSetTableModel paramSetsTableModel;

    public ParameterSetViewPanel() {
        this(null);
    }

    public ParameterSetViewPanel(Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap) {
        this.parameterSetsMap = parameterSetsMap;

        initGUI();
    }

    private void viewParamSetButtonActionPerformed(ActionEvent evt) {
        log.debug("viewParamSetButton.actionPerformed, event=" + evt);

        int selectedModelRow = paramSetsTable
            .convertRowIndexToModel(paramSetsTable.getSelectedRow());

        if (selectedModelRow == -1) {
            MessageUtil.showError(this, "No parameter set selected");
        } else {
            ParameterSet paramSet = paramSetsTableModel.getParamSetAtRow(selectedModelRow);

            ViewParametersDialog.viewParameters(paramSet);
        }
    }

    private void initGUI() {
        try {
            GridBagLayout thisLayout = new GridBagLayout();
            setPreferredSize(new Dimension(400, 300));
            thisLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            thisLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            thisLayout.columnWeights = new double[] { 0.1 };
            thisLayout.columnWidths = new int[] { 7 };
            setLayout(thisLayout);
            this.add(getParamSetsScrollPane(), new GridBagConstraints(0, 0, 1, 3, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            this.add(getButtonPanel(), new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getParamSetsScrollPane() {
        if (paramSetsScrollPane == null) {
            paramSetsScrollPane = new JScrollPane();
            paramSetsScrollPane.setViewportView(getParamSetsTable());
        }
        return paramSetsScrollPane;
    }

    private ZTable getParamSetsTable() {
        if (paramSetsTable == null) {
            paramSetsTableModel = new ParameterSetTableModel(parameterSetsMap);
            paramSetsTable = new ZTable();
            paramSetsTable.setModel(paramSetsTableModel);
        }
        return paramSetsTable;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            buttonPanel.add(getViewParamSetButton());
        }
        return buttonPanel;
    }

    private JButton getViewParamSetButton() {
        if (viewParamSetButton == null) {
            viewParamSetButton = new JButton();
            viewParamSetButton.setText("view parameters");
            viewParamSetButton.addActionListener(this::viewParamSetButtonActionPerformed);
        }
        return viewParamSetButton;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new ParameterSetViewPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
