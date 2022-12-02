package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.Date;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.ReportFilePaths;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.ops.parameters.ParameterSetViewDialog;
import gov.nasa.ziggy.ui.ops.parameters.ParameterSetViewPanel;
import gov.nasa.ziggy.ui.proxy.PipelineInstanceCrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class InstanceDetailsDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(InstanceDetailsDialog.class);

    private JPanel infoPanel;
    private JPanel nodesPanel;
    private JPanel actionPanel;
    private JButton viewNodeParamsButton;
    private JPanel nodesButtonPanel;
    private JScrollPane nodesScrollPane;
    private JButton closeButton;
    private ParameterSetViewPanel pipelineParameterSetsPanel;
    private JLabel endLabel;
    private JTextField endTextField;
    private JTextField totalTextField;
    private JTextField startTextField;
    private JTextField idTextField;
    private JTextField nameTextField;
    private JButton updateButton;
    private JButton reportButton;
    private ZTable nodeTable;
    private JLabel idLabel;
    private JLabel totalLabel;
    private JLabel startLabel;
    private JLabel nameLabel;

    private PipelineInstance pipelineInstance = null;

    private InstanceModulesTableModel nodeTableModel;

    /* for Jigloo use only */
    public InstanceDetailsDialog(JFrame frame) {
        this(frame, null);
    }

    public InstanceDetailsDialog(JFrame frame, PipelineInstance pipelineInstance) {
        super(frame, true);
        this.pipelineInstance = pipelineInstance;

        initGUI();
    }

    private void updateButtonActionPerformed(ActionEvent evt) {
        log.debug("updateButton.actionPerformed, event=" + evt);

        try {
            String newName = nameTextField.getText();

            if (!newName.equals(pipelineInstance.getName())) {
                PipelineInstanceCrudProxy instanceCrud = new PipelineInstanceCrudProxy();
                instanceCrud.updateName(pipelineInstance.getId(), newName);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void viewNodeParamsButtonActionPerformed(ActionEvent evt) {
        log.debug("viewNodeParamsButton.actionPerformed, event=" + evt);

        int selectedRow = nodeTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No module selected");
        } else {
            PipelineInstanceNode node = nodeTableModel.getPipelineNodeAt(selectedRow);
            ParameterSetViewDialog.showParameterSet(this, node.getModuleParameterSets());
        }
    }

    private void reportButtonActionPerformed(ActionEvent evt) {
        log.debug("reportButton.actionPerformed, event=" + evt);

        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generatePedigreeReport(pipelineInstance);

        TextualReportDialog.showReport(this, report,
            ReportFilePaths.instanceDetailsReportPath(pipelineInstance.getId()));
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.debug("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                GridBagLayout thisLayout = new GridBagLayout();
                setTitle("Pipeline Instance Details");
                thisLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                    0.1, 0.1, 0.1, 0.1 };
                thisLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
                thisLayout.columnWeights = new double[] { 0.1 };
                thisLayout.columnWidths = new int[] { 7 };
                getContentPane().setLayout(thisLayout);
                getContentPane().add(getInfoPanel(),
                    new GridBagConstraints(0, 0, 1, 4, 0.0, 0.0, GridBagConstraints.CENTER,
                        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
                getContentPane().add(getPipelineParameterSetsPanel(),
                    new GridBagConstraints(0, 4, 1, 4, 0.0, 1.0, GridBagConstraints.CENTER,
                        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
                getContentPane().add(getNodesPanel(),
                    new GridBagConstraints(0, 8, 1, 4, 0.0, 1.0, GridBagConstraints.CENTER,
                        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
                getContentPane().add(getActionPanel(),
                    new GridBagConstraints(0, 12, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            }
            this.setSize(520, 648);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getInfoPanel() {
        if (infoPanel == null) {
            infoPanel = new JPanel();
            GridBagLayout infoPanelLayout = new GridBagLayout();
            infoPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7 };
            infoPanelLayout.rowHeights = new int[] { 7, 7, 7, 7 };
            infoPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            infoPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1 };
            infoPanel.setLayout(infoPanelLayout);
            infoPanel.setBorder(BorderFactory.createTitledBorder("Pipeline Instance"));
            infoPanel.add(getNameLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getStartLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getEndLabel(),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getTotalLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getIdLabel(),
                new GridBagConstraints(6, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getNameTextField(),
                new GridBagConstraints(1, 0, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getIdTextField(),
                new GridBagConstraints(7, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getStartTextField(),
                new GridBagConstraints(1, 1, 7, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getEndTextField(),
                new GridBagConstraints(1, 2, 8, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getTotalTextField(),
                new GridBagConstraints(1, 3, 8, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            infoPanel.add(getUpdateButton(), new GridBagConstraints(5, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return infoPanel;
    }

    private ParameterSetViewPanel getPipelineParameterSetsPanel() {
        if (pipelineParameterSetsPanel == null) {
            if (pipelineInstance != null) {
                pipelineParameterSetsPanel = new ParameterSetViewPanel(
                    pipelineInstance.getPipelineParameterSets());
            } else {
                pipelineParameterSetsPanel = new ParameterSetViewPanel();
            }
            pipelineParameterSetsPanel
                .setBorder(BorderFactory.createTitledBorder("Pipeline Parameters"));
        }
        return pipelineParameterSetsPanel;
    }

    private JPanel getNodesPanel() {
        if (nodesPanel == null) {
            nodesPanel = new JPanel();
            BorderLayout nodesPanelLayout = new BorderLayout();
            nodesPanel.setLayout(nodesPanelLayout);
            nodesPanel.setBorder(BorderFactory.createTitledBorder("Modules"));
            nodesPanel.add(getNodesScrollPane(), BorderLayout.CENTER);
            nodesPanel.add(getNodesButtonPanel(), BorderLayout.SOUTH);
        }
        return nodesPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(35);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getCloseButton());
            actionPanel.add(getReportButton());
        }
        return actionPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(this::closeButtonActionPerformed);
        }
        return closeButton;
    }

    private JScrollPane getNodesScrollPane() {
        if (nodesScrollPane == null) {
            nodesScrollPane = new JScrollPane();
            nodesScrollPane.setViewportView(getNodeTable());
        }
        return nodesScrollPane;
    }

    private JPanel getNodesButtonPanel() {
        if (nodesButtonPanel == null) {
            nodesButtonPanel = new JPanel();
            nodesButtonPanel.add(getViewNodeParamsButton());
        }
        return nodesButtonPanel;
    }

    private JButton getViewNodeParamsButton() {
        if (viewNodeParamsButton == null) {
            viewNodeParamsButton = new JButton();
            viewNodeParamsButton.setText("view module parameters");
            viewNodeParamsButton.addActionListener(this::viewNodeParamsButtonActionPerformed);
        }
        return viewNodeParamsButton;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            InstanceDetailsDialog inst = new InstanceDetailsDialog(frame);
            inst.setVisible(true);
        });
    }

    private JLabel getNameLabel() {
        if (nameLabel == null) {
            nameLabel = new JLabel();
            nameLabel.setText("Name ");
        }
        return nameLabel;
    }

    private JLabel getStartLabel() {
        if (startLabel == null) {
            startLabel = new JLabel();
            startLabel.setText("Start ");
        }
        return startLabel;
    }

    private JLabel getEndLabel() {
        if (endLabel == null) {
            endLabel = new JLabel();
            endLabel.setText("End ");
        }
        return endLabel;
    }

    private JLabel getTotalLabel() {
        if (totalLabel == null) {
            totalLabel = new JLabel();
            totalLabel.setText("Total ");
        }
        return totalLabel;
    }

    private JLabel getIdLabel() {
        if (idLabel == null) {
            idLabel = new JLabel();
            idLabel.setText("ID ");
        }
        return idLabel;
    }

    private JTextField getNameTextField() {
        if (nameTextField == null) {
            nameTextField = new JTextField();
            nameTextField.setText(pipelineInstance.getName());
        }
        return nameTextField;
    }

    private JTextField getIdTextField() {
        if (idTextField == null) {
            idTextField = new JTextField();
            idTextField.setText(pipelineInstance.getId() + "");
            idTextField.setEditable(false);
        }
        return idTextField;
    }

    private JTextField getStartTextField() {
        if (startTextField == null) {
            startTextField = new JTextField();
            startTextField.setText(pipelineInstance.getStartProcessingTime().toString());
            startTextField.setEditable(false);
        }
        return startTextField;
    }

    private JTextField getEndTextField() {
        if (endTextField == null) {
            endTextField = new JTextField();
            Date endProcessingTime = pipelineInstance.getEndProcessingTime();
            if (endProcessingTime.getTime() == 0) {
                endTextField.setText("-");
            } else {
                endTextField.setText(endProcessingTime.toString());
            }
            endTextField.setEditable(false);
        }
        return endTextField;
    }

    private JTextField getTotalTextField() {
        if (totalTextField == null) {
            totalTextField = new JTextField();
            String elapsedTime = pipelineInstance.elapsedTime();
            totalTextField.setText(elapsedTime);
            totalTextField.setEditable(false);
        }
        return totalTextField;
    }

    private ZTable getNodeTable() {
        if (nodeTable == null) {
            nodeTableModel = new InstanceModulesTableModel(pipelineInstance);
            nodeTable = new ZTable();
            nodeTable.setRowShadingEnabled(true);
            nodeTable.setModel(nodeTableModel);
        }
        return nodeTable;
    }

    private JButton getReportButton() {
        if (reportButton == null) {
            reportButton = new JButton();
            reportButton.setText("report");
            reportButton.addActionListener(this::reportButtonActionPerformed);
        }
        return reportButton;
    }

    private JButton getUpdateButton() {
        if (updateButton == null) {
            updateButton = new JButton();
            updateButton.setText("update");
            updateButton.addActionListener(this::updateButtonActionPerformed);
        }
        return updateButton;
    }
}
