package gov.nasa.ziggy.ui.parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.IMPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REPORT;

import java.awt.Cursor;
import java.awt.event.ActionEvent;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.TableModelContentClass;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ParameterSetCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ParametersOperationsProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;
import gov.nasa.ziggy.ui.util.proxy.RetrieveLatestVersionsCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * View / Edit panel for {@link ParameterSet} instances. The user can also use this panel to move
 * parameter sets into {@link Group}s, which can then be expanded or collapsed.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditParameterSetsPanel extends AbstractViewEditPanel<ParameterSet> {

    private static final long serialVersionUID = 20230810L;

    private ParameterSetCrudProxy parameterSetCrud = new ParameterSetCrudProxy();
    private String defaultParamLibImportExportPath;

    /**
     * Convenience method that can be used instead of the constructor. Helpful because the row model
     * for parameter sets needs the tree model in its constructor.
     */
    public static ViewEditParameterSetsPanel newInstance() {
        ZiggyTreeModel<ParameterSet> treeModel = new ZiggyTreeModel<>(new ParameterSetCrudProxy());
        ParameterSetsRowModel rowModel = new ParameterSetsRowModel(treeModel);
        return new ViewEditParameterSetsPanel(rowModel, treeModel);
    }

    public ViewEditParameterSetsPanel(RowModel rowModel, ZiggyTreeModel<ParameterSet> treeModel) {
        super(rowModel, treeModel, "Name");
        buildComponent();

        ZiggySwingUtils.addButtonsToPanel(getButtonPanel(),
            ZiggySwingUtils.createButton(REPORT, this::report),
            ZiggySwingUtils.createButton(IMPORT, this::importParameterLibrary),
            ZiggySwingUtils.createButton(EXPORT, this::exportParameterLibrary));

        for (int column = 0; column < ParameterSetsRowModel.COLUMN_WIDTHS.length; column++) {
            ziggyTable.setPreferredColumnWidth(column, ParameterSetsRowModel.COLUMN_WIDTHS[column]);
        }
    }

    private void report(ActionEvent evt) {

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_MONITOR);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        Object[] options = { "Formatted", "Colon-delimited" };
        int n = JOptionPane.showOptionDialog(SwingUtilities.getWindowAncestor(this),
            "Specify report type", "Report type", JOptionPane.YES_NO_CANCEL_OPTION,
            JOptionPane.QUESTION_MESSAGE, null, options, options[0]);

        boolean csvMode = n != 0;

        PipelineOperationsProxy ops = new PipelineOperationsProxy();
        String report = ops.generateParameterLibraryReport(csvMode);

        TextualReportDialog.showReport(SwingUtilities.getWindowAncestor(this), report,
            "Parameter report");
    }

    private void importParameterLibrary(ActionEvent evt) {

        try {
            JFileChooser fc = new JFileChooser(defaultParamLibImportExportPath);

            fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
            fc.setDialogTitle("Select parameter library file to import");

            if (fc.showDialog(this, "Import") == JFileChooser.APPROVE_OPTION) {
                File file = fc.getSelectedFile();
                defaultParamLibImportExportPath = file.getAbsolutePath();

                ParametersOperationsProxy paramsOps = new ParametersOperationsProxy();
                List<ParameterSetDescriptor> dryRunResults = paramsOps
                    .importParameterLibrary(file.getAbsolutePath(), null, true);

                List<String> excludeList = ImportParamLibDialog
                    .selectParamSet(SwingUtilities.getWindowAncestor(this), dryRunResults);

                if (excludeList != null) { // null means user cancelled
                    paramsOps.importParameterLibrary(file.getAbsolutePath(), excludeList, false);
                }
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void exportParameterLibrary(ActionEvent evt) {

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
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected Set<OptionalViewEditFunctions> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunctions.DELETE, OptionalViewEditFunctions.NEW,
            OptionalViewEditFunctions.COPY, OptionalViewEditFunctions.RENAME,
            OptionalViewEditFunctions.GROUP);
    }

    @Override
    protected RetrieveLatestVersionsCrudProxy<ParameterSet> getCrudProxy() {
        return parameterSetCrud;
    }

    @Override
    protected void copy(int row) {
        checkPrivileges();

        ParameterSet selectedParameterSet = ziggyTable.getContentAtViewRow(row);

        try {
            String newParameterSetName = JOptionPane.showInputDialog(
                SwingUtilities.getWindowAncestor(this), "Enter the name for the new parameter set",
                "New parameter set", JOptionPane.PLAIN_MESSAGE);
            if (newParameterSetName == null) {
                return;
            }
            if (newParameterSetName.isEmpty()) {
                MessageUtil.showError(this, "Please enter a parameter set name");
                return;
            }
            ParameterSet newParameterSet = selectedParameterSet.newInstance();
            newParameterSet.rename(newParameterSetName);
            showEditDialog(newParameterSet, true);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(ParameterSet module, boolean isNew) {
        try {
            EditParameterSetDialog dialog = new EditParameterSetDialog(
                SwingUtilities.getWindowAncestor(this), module, isNew);
            dialog.setVisible(true);

            if (!dialog.isCancelled()) {
                ziggyTable.loadFromDatabase();
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void rename(int row) {
        checkPrivileges();

        ParameterSet selectedParameterSet = ziggyTable.getContentAtViewRow(row);

        try {
            String newParameterSetName = JOptionPane.showInputDialog(
                SwingUtilities.getWindowAncestor(this), "Enter the new name for this parameter set",
                "Rename parameter set", JOptionPane.PLAIN_MESSAGE);
            if (newParameterSetName == null) {
                return;
            }
            if (newParameterSetName.isEmpty()) {
                MessageUtil.showError(this, "Please enter a parameter set name");
                return;
            }
            parameterSetCrud.rename(selectedParameterSet, newParameterSetName);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void edit(int row) {
        checkPrivileges();

        ParameterSet selectedParameterSet = ziggyTable.getContentAtViewRow(row);
        if (selectedParameterSet != null) {
            showEditDialog(selectedParameterSet, false);
        }
    }

    @Override
    protected void delete(int row) {
        checkPrivileges();

        ParameterSet selectedParameterSet = ziggyTable.getContentAtViewRow(row);
        if (selectedParameterSet.isLocked()) {
            MessageUtil.showError(this,
                "Can't delete a locked parameter set. Parameter sets are locked "
                    + "when referenced by a pipeline instance");
            return;
        }
        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete parameter set '" + selectedParameterSet.getName()
                + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                parameterSetCrud.delete(selectedParameterSet);
                ziggyTable.loadFromDatabase();
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    @Override
    protected void create() {
        checkPrivileges();

        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        ParameterSet newParameterSet = NewParameterSetDialog.createParameterSet(this);
        setCursor(null);

        if (newParameterSet == null) {
            return;
        }
        parameterSetCrud.save(newParameterSet);
        ziggyTable.loadFromDatabase();
    }

    @Override
    public void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    /**
     * Implementation of {@link RowModel} for a NetBeans {@link Outline} of {@link ParameterSet}
     * instances.
     *
     * @author PT
     */
    private static class ParameterSetsRowModel
        implements RowModel, TableModelContentClass<ParameterSet> {

        private ZiggyTreeModel<ParameterSet> treeModel;

        private static final String[] COLUMN_NAMES = { "Type", "Version", "Locked", "User",
            "Modified" };
        private static final Class<?>[] COLUMN_CLASSES = { String.class, Integer.class,
            Boolean.class, String.class, Object.class };
        private static final int[] COLUMN_WIDTHS = { 250, 300, 30, 30, 100, 150 };

        public ParameterSetsRowModel(ZiggyTreeModel<ParameterSet> treeModel) {
            this.treeModel = treeModel;
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            checkColumnArgument(columnIndex);
            return COLUMN_CLASSES[columnIndex];
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int column) {
            checkColumnArgument(column);
            return COLUMN_NAMES[column];
        }

        private void checkColumnArgument(int columnIndex) {
            checkArgument(columnIndex >= 0 && columnIndex < COLUMN_NAMES.length, "column value of "
                + columnIndex + " outside of expected range from 0 to " + COLUMN_NAMES.length);
        }

        @Override
        public Object getValueFor(Object treeNode, int columnIndex) {
            checkColumnArgument(columnIndex);

            treeModel.validityCheck();
            Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();

            if (!(node instanceof ParameterSet)) {
                return null;
            }
            ParameterSet parameterSet = (ParameterSet) node;

            AuditInfo auditInfo = parameterSet.getAuditInfo();

            User lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> shortClassname(parameterSet.getClassname());
                case 1 -> parameterSet.getVersion();
                case 2 -> parameterSet.isLocked();
                case 3 -> lastChangedUser != null ? lastChangedUser : "---";
                case 4 -> lastChangedTime != null ? lastChangedTime : "---";
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        private String shortClassname(String longClassname) {
            String[] splitClassname = longClassname.split("\\.");
            return splitClassname[splitClassname.length - 1];
        }

        @Override
        public boolean isCellEditable(Object node, int column) {
            return false;
        }

        @Override
        public void setValueFor(Object node, int column, Object value) {
            // Not editable.
        }

        @Override
        public Class<ParameterSet> tableModelContentClass() {
            return ParameterSet.class;
        }
    }
}
