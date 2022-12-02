package gov.nasa.ziggy.ui.config.parameters;

import java.awt.event.ActionEvent;
import java.io.File;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractClonableViewEditPanel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.ParameterSetCrudProxy;
import gov.nasa.ziggy.ui.proxy.ParametersOperationsProxy;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetsViewEditPanel extends AbstractClonableViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetsViewEditPanel.class);

    // do NOT init to null!
    // (see getTableModel)
    private ParameterSetsTableModel parameterSetsTableModel;

    private final ParameterSetCrudProxy parameterSetCrud;

    private JButton reportButton;
    private JButton importButton;
    private JButton exportButton;

    private static String defaultParamLibImportExportPath = null;

    public ParameterSetsViewEditPanel() throws PipelineUIException {
        super(true, true);

        parameterSetCrud = new ParameterSetCrudProxy();

        initGUI();

        getButtonPanel().add(getReportButton());
        getButtonPanel().add(getImportButton());
        getButtonPanel().add(getExportButton());
    }

    private void reportButtonActionPerformed(ActionEvent evt) {
        log.debug("reportButton.actionPerformed, event=" + evt);

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        Object[] options = { "Formatted", "Colon-delimited" };
        int n = ZiggyGuiConsole.showOptionDialog("Specify report type", "ReportType",
            JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options,
            options[0]);

        boolean csvMode = n == 0 ? false : true;

        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generateParameterLibraryReport(csvMode);

        ZiggyGuiConsole.showReport(report);
    }

    private void importButtonActionPerformed(ActionEvent evt) {
        log.debug("importButton.actionPerformed, event=" + evt);

        try {
            JFileChooser fc = new JFileChooser(defaultParamLibImportExportPath);

            fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
            fc.setDialogTitle("Select parameter library file to import");

            int returnVal = fc.showDialog(this, "Import");

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();
                defaultParamLibImportExportPath = file.getAbsolutePath();

                ParametersOperationsProxy paramsOps = new ParametersOperationsProxy();
                List<ParameterSetDescriptor> dryRunResults = paramsOps
                    .importParameterLibrary(file.getAbsolutePath(), null, true);

                List<String> excludeList = ZiggyGuiConsole.selectParamSet(dryRunResults);

                if (excludeList != null) { // null means user cancelled
                    paramsOps.importParameterLibrary(file.getAbsolutePath(), excludeList, false);
                }
            }
        } catch (Exception e) {
            log.warn("caught e = ", e);
            MessageUtil.showError(this, e);
        }
    }

    private void exportButtonActionPerformed(ActionEvent evt) {
        log.debug("exportButton.actionPerformed, event=" + evt);

        try {
            JFileChooser fc = new JFileChooser(defaultParamLibImportExportPath);
            fc.setDialogTitle("Select the destination file for the parameter library export");

            fc.setFileSelectionMode(JFileChooser.FILES_ONLY);

            int returnVal = fc.showDialog(this, "Export");

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();
                defaultParamLibImportExportPath = file.getAbsolutePath();

                ParametersOperationsProxy paramsOps = new ParametersOperationsProxy();
                // excludes not supported ATM
                paramsOps.exportParameterLibrary(file.getAbsolutePath(), null, false);
            }
        } catch (Exception e) {
            log.warn("caught e = ", e);
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (parameterSetsTableModel == null) {
            parameterSetsTableModel = new ParameterSetsTableModel();
            parameterSetsTableModel.register();
        }

        log.debug("getTableModel() - end");
        return parameterSetsTableModel;
    }

    @Override
    protected void doNew() {
        log.debug("doNew() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        ParameterSet newParameterSet = ParameterSetNewDialog.createParameterSet();

        if (newParameterSet != null) {
            ParameterSetCrudProxy paramSetCrud = new ParameterSetCrudProxy();
            paramSetCrud.save(newParameterSet);
            parameterSetsTableModel.loadFromDatabase();
        } else {
            // user cancelled
            return;
        }

        log.debug("doNew() - end");
    }

    @Override
    protected void doClone(int row) {
        log.debug("doClone() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        ParameterSet selectedParameterSet = parameterSetsTableModel.getParamSetAtRow(row);

        try {
            String newParameterSetName = ZiggyGuiConsole.showInputDialog(
                "Enter the name for the new Parameter Set", "New Parameter Set",
                JOptionPane.PLAIN_MESSAGE);

            if (newParameterSetName == null || newParameterSetName.length() == 0) {
                MessageUtil.showError(this, "Please enter a Parameter Set name");
                return;
            }

            ParameterSet newParameterSet = new ParameterSet(selectedParameterSet);
            newParameterSet.rename(newParameterSetName);

            showEditDialog(newParameterSet, true);

            parameterSetsTableModel.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doClone() - end");
    }

    @Override
    protected void doRename(int row) {
        log.debug("doRename() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        ParameterSet selectedParameterSet = parameterSetsTableModel.getParamSetAtRow(row);

        try {
            String newParameterSetName = ZiggyGuiConsole.showInputDialog(
                "Enter the new name for this Parameter Set", "Rename Parameter Set",
                JOptionPane.PLAIN_MESSAGE);

            if (newParameterSetName == null || newParameterSetName.length() == 0) {
                MessageUtil.showError(this, "Please enter a Parameter Set name");
                return;
            }

            parameterSetCrud.rename(selectedParameterSet, newParameterSetName);
            parameterSetsTableModel.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doRename() - end");
    }

    @Override
    protected void doEdit(int row) {
        log.debug("doEdit(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        ParameterSet parameterSet = parameterSetsTableModel.getParamSetAtRow(row);
        showEditDialog(parameterSet, false);

        log.debug("doEdit(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            parameterSetsTableModel.loadFromDatabase();
        } catch (Exception e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void doDelete(int row) {
        log.debug("doDelete(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        ParameterSet parameterSet = parameterSetsTableModel.getParamSetAtRow(row);

        if (!parameterSet.isLocked()) {
            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete Parameter Set '" + parameterSet.getName() + "'?");

            if (choice == JOptionPane.YES_OPTION) {
                try {
                    parameterSetCrud.delete(parameterSet);
                    parameterSetsTableModel.loadFromDatabase();
                } catch (Throwable e) {
                    MessageUtil.showError(this, e);
                }
            }
        } else {
            MessageUtil.showError(this,
                "Can't delete a locked parameter set. Parameter sets are locked"
                    + "when referenced by a pipeline instance");
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected String getEditMenuText() {
        return "Edit selected Parameter Set...";
    }

    @Override
    protected String getNewMenuText() {
        return "Create new Parameter Set...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected Parameter Set...";
    }

    @Override
    protected String getCloneMenuText() {
        return "Clone selected Parameter Set";
    }

    @Override
    protected String getRenameMenuText() {
        return "Rename selected Parameter Set";
    }

    private void showEditDialog(ParameterSet module, boolean isNew) {
        log.debug("showEditDialog() - start");

        try {
            ParameterSetEditDialog inst = ZiggyGuiConsole.newParameterSetEditDialog(module, isNew);
            inst.setVisible(true);

            if (!inst.isCancelled()) {
                parameterSetsTableModel.loadFromDatabase();
            }
        } catch (Exception e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
    }

    private JButton getReportButton() {
        if (reportButton == null) {
            reportButton = new JButton();
            reportButton.setText("report");
            reportButton.addActionListener(this::reportButtonActionPerformed);
        }
        return reportButton;
    }

    private JButton getImportButton() {
        if (importButton == null) {
            importButton = new JButton();
            importButton.setText("import");
            importButton.addActionListener(this::importButtonActionPerformed);
        }

        return importButton;
    }

    private JButton getExportButton() {
        if (exportButton == null) {
            exportButton = new JButton();
            exportButton.setText("export");
            exportButton.addActionListener(this::exportButtonActionPerformed);
        }

        return exportButton;
    }
}
