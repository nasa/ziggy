package gov.nasa.ziggy.ui.parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.IMPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REPORT;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.RowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterImportExportOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messages.ParametersChangedMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditGroupPanel;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * View / Edit panel for {@link ParameterSet} instances. The user can also use this panel to move
 * parameter sets into {@link Group}s, which can then be expanded or collapsed.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditParameterSetsPanel extends AbstractViewEditGroupPanel<ParameterSet> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ViewEditParameterSetsPanel.class);
    private static final long serialVersionUID = 20241002L;
    private static final String TYPE = "ParameterSet";

    private String defaultParamLibImportExportPath;

    private final ParameterImportExportOperations parameterImportExportOperations = new ParameterImportExportOperations();

    public ViewEditParameterSetsPanel(RowModel rowModel, ZiggyTreeModel<ParameterSet> treeModel) {
        super(rowModel, treeModel, "Name");
        buildComponent();

        for (int column = 0; column < ParameterSetsRowModel.COLUMN_WIDTHS.length; column++) {
            ziggyTable.setPreferredColumnWidth(column, ParameterSetsRowModel.COLUMN_WIDTHS[column]);
        }

        ziggyTable.getTable()
            .getSelectionModel()
            .setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

        ZiggyMessenger.subscribe(InvalidateConsoleModelsMessage.class, this::invalidateModel);
        ZiggyMessenger.subscribe(ParametersChangedMessage.class, this::invalidateModel);
    }

    /**
     * Convenience method that can be used instead of the constructor. Helpful because the row model
     * for parameter sets needs the tree model in its constructor.
     */
    public static ViewEditParameterSetsPanel newInstance() {
        ZiggyTreeModel<ParameterSet> treeModel = new ZiggyTreeModel<>(TYPE,
            () -> new ParametersOperations().parameterSets());
        ParameterSetsRowModel rowModel = new ParameterSetsRowModel();
        return new ViewEditParameterSetsPanel(rowModel, treeModel);
    }

    private void invalidateModel(PipelineMessage message) {
        ziggyTable.loadFromDatabase();
    }

    @Override
    protected List<JButton> buttons() {
        List<JButton> buttons = new ArrayList<>(
            List.of(ZiggySwingUtils.createButton(REPORT, this::report),
                ZiggySwingUtils.createButton(IMPORT, this::importParameterLibrary),
                ZiggySwingUtils.createButton(EXPORT, this::exportParameterLibrary)));
        buttons.addAll(super.buttons());
        return buttons;
    }

    @Override
    protected void updateActionState(Map<OptionalViewEditFunction, Action> actionByFunction) {
        actionByFunction.get(OptionalViewEditFunction.EDIT)
            .setEnabled(ziggyTable.getTable().getSelectedRowCount() == 1);
    }

    private void report(ActionEvent evt) {

        Object[] options = { "Formatted", "Colon-delimited" };
        int n = JOptionPane.showOptionDialog(SwingUtilities.getWindowAncestor(this),
            "Specify report type", "Report type", JOptionPane.YES_NO_CANCEL_OPTION,
            JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
        boolean csvMode = n != 0;

        new SwingWorker<String, String>() {
            @Override
            protected String doInBackground() throws Exception {
                return new PipelineReportGenerator().generateParameterLibraryReport(csvMode);
            }

            @Override
            protected void done() {
                try {
                    TextualReportDialog.showReport(
                        SwingUtilities.getWindowAncestor(ViewEditParameterSetsPanel.this), get(),
                        "Parameter report");
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(getRootPane(), e);
                }
            }
        }.execute();
    }

    private void importParameterLibrary(ActionEvent evt) {

        JFileChooser fc = new JFileChooser(defaultParamLibImportExportPath);
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fc.setDialogTitle("Select parameter library file to import");
        if (fc.showDialog(this, "Import") != JFileChooser.APPROVE_OPTION) {
            return;
        }

        new SwingWorker<List<ParameterSetDescriptor>, Void>() {
            @Override
            protected List<ParameterSetDescriptor> doInBackground() throws Exception {
                defaultParamLibImportExportPath = fc.getSelectedFile().getAbsolutePath();

                return parameterImportExportOperations().importParameterLibrary(
                    fc.getSelectedFile().getAbsolutePath(), null, ParamIoMode.DRYRUN);
            }

            @Override
            protected void done() {
                try {
                    List<String> excludeList = ImportParamLibDialog.selectParamSet(
                        SwingUtilities.getWindowAncestor(ViewEditParameterSetsPanel.this), get());

                    // null means user cancelled.
                    if (excludeList == null) {
                        return;
                    }

                    new SwingWorker<Void, Void>() {
                        @Override
                        protected Void doInBackground() throws Exception {
                            parameterImportExportOperations().importParameterLibrary(
                                defaultParamLibImportExportPath, excludeList, ParamIoMode.STANDARD);
                            return null;
                        }

                        @Override
                        protected void done() {
                            try {
                                get(); // check for exception
                            } catch (InterruptedException | ExecutionException e) {
                                MessageUtils.showError(ViewEditParameterSetsPanel.this, e);
                            }
                        }
                    }.execute();
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(ViewEditParameterSetsPanel.this, e);
                }
            }
        }.execute();
    }

    private void exportParameterLibrary(ActionEvent evt) {

        JFileChooser fc = new JFileChooser(defaultParamLibImportExportPath);
        fc.setDialogTitle("Select the destination file for the parameter library export");
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        if (fc.showDialog(this, "Export") != JFileChooser.APPROVE_OPTION) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                // Excludes not supported ATM.
                defaultParamLibImportExportPath = fc.getSelectedFile().getAbsolutePath();
                parameterImportExportOperations().exportParameterLibrary(
                    defaultParamLibImportExportPath, null, ParamIoMode.STANDARD);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(ViewEditParameterSetsPanel.this, e);
                }
            }
        }.execute();
    }

    @Override
    public void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void edit(int row) {
        try {
            EditParameterSetDialog dialog = new EditParameterSetDialog(
                SwingUtilities.getWindowAncestor(this), ziggyTable.getContentAtViewRow(row), false);
            dialog.setVisible(true);

            if (!dialog.isCancelled()) {
                ziggyTable.loadFromDatabase();
            }
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected String getType() {
        return TYPE;
    }

    private ParameterImportExportOperations parameterImportExportOperations() {
        return parameterImportExportOperations;
    }

    /**
     * Implementation of {@link RowModel} for a NetBeans {@link Outline} of {@link ParameterSet}
     * instances.
     *
     * @author PT
     */
    private static class ParameterSetsRowModel
        implements RowModel, ModelContentClass<ParameterSet> {

        private static final String[] COLUMN_NAMES = { "Version", "Locked", "User", "Modified" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, Boolean.class,
            String.class, Object.class };
        private static final int[] COLUMN_WIDTHS = { 250, 30, 30, 100, 150 };

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
            checkArgument(columnIndex >= 0 && columnIndex < COLUMN_NAMES.length, "Column value of "
                + columnIndex + " outside of expected range from 0 to " + COLUMN_NAMES.length);
        }

        @Override
        public Object getValueFor(Object treeNode, int columnIndex) {
            checkColumnArgument(columnIndex);

            Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();

            if (!(node instanceof ParameterSet)) {
                return null;
            }
            ParameterSet parameterSet = (ParameterSet) node;

            AuditInfo auditInfo = parameterSet.getAuditInfo();

            String lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> parameterSet.getVersion();
                case 1 -> parameterSet.isLocked();
                case 2 -> lastChangedUser != null ? lastChangedUser : "---";
                case 3 -> lastChangedTime != null ? lastChangedTime : "---";
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
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
