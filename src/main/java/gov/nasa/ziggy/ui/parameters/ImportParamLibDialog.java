package gov.nasa.ziggy.ui.parameters;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.IMPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REPORT;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

import org.netbeans.swing.etable.ETable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.parameters.ParameterSetDescriptor.State;
import gov.nasa.ziggy.ui.util.TextualReportDialog;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.StringUtils;

/**
 * This dialog is used to import a parameter library from disk into the parameter library in the
 * database. It is initialized by running the parameter library importer in "dry run" mode, which
 * tells the user what actions will take effect when the import is run for real. The user can also
 * choose to exclude some of the parameter sets from the import if desired.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ImportParamLibDialog extends JDialog {
    private static final Logger log = LoggerFactory.getLogger(ImportParamLibDialog.class);

    private ETable paramImportTable;
    private ParamLibImportTableModel paramImportTableModel;
    private boolean cancelled;
    private List<ParameterSetDescriptor> dryRunResults;

    public ImportParamLibDialog(Window owner, List<ParameterSetDescriptor> dryRunResults) {
        super(owner, DEFAULT_MODALITY_TYPE);
        paramImportTableModel = new ParamLibImportTableModel(dryRunResults);
        this.dryRunResults = dryRunResults;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Import parameter library");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(ZiggySwingUtils.createButtonPanel(createButton(REPORT, this::report),
            createButton(IMPORT, this::importParameterLibrary), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        setMinimumSize(new Dimension(700, 525));
        pack();
    }

    private JPanel createDataPanel() {
        paramImportTable = new ZiggyTable<>(paramImportTableModel).getTable();
        paramImportTable.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        paramImportTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent evt) {
                tableMouseClicked(evt, paramImportTable);
            }
        });

        JPanel dataPanel = new JPanel();
        dataPanel.setLayout(new BorderLayout());
        dataPanel.add(new JScrollPane(paramImportTable), BorderLayout.CENTER);
        return dataPanel;
    }

    private void tableMouseClicked(MouseEvent evt, ETable table) {
        if (evt.getClickCount() == 2) {
            int tableRow = table.rowAtPoint(evt.getPoint());
            int selectedModelRow = table.convertRowIndexToModel(tableRow);
            log.info("[DC] selectedModelRow={}", selectedModelRow);

            ParameterSetDescriptor desc = paramImportTableModel.getContentAtRow(selectedModelRow);
            String report = "Library Version:\n\n" + desc.getLibraryProps()
                + "\n\nImport Version:\n\n" + desc.getFileProps();
            TextualReportDialog.showReport(this, report, "Import report");
        }
    }

    private void importParameterLibrary(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    private void report(ActionEvent evt) {

        StringBuilder report = new StringBuilder();

        List<String> excludes = paramImportTableModel.getExcludeList();
        Map<ParameterSetDescriptor.State, List<ParameterSetDescriptor>> binnedByState = new HashMap<>();

        for (ParameterSetDescriptor d : dryRunResults) {
            String name = d.getName();
            State descState = d.getState();

            if (excludes.contains(name)) {
                // override state since the user de-selected this one for import
                descState = ParameterSetDescriptor.State.IGNORE;
            }

            List<ParameterSetDescriptor> list = binnedByState.get(descState);
            if (list == null) {
                list = new LinkedList<>();
                binnedByState.put(descState, list);
            }
            list.add(d);
        }

        report.append("Parameter sets to be created:\n\n");
        appendToReport(report, binnedByState.get(ParameterSetDescriptor.State.CREATE));
        report.append("Parameter sets to be updated:\n\n");
        appendToReport(report, binnedByState.get(ParameterSetDescriptor.State.UPDATE));
        report.append("Parameter sets in the library, but not in the export:\n\n");
        appendToReport(report, binnedByState.get(ParameterSetDescriptor.State.LIBRARY_ONLY));
        report.append("Parameter sets that are the same (no change needed):\n\n");
        appendToReport(report, binnedByState.get(ParameterSetDescriptor.State.SAME));
        report.append("Parameter sets to be ignored (on the exclude list):\n\n");
        appendToReport(report, binnedByState.get(ParameterSetDescriptor.State.IGNORE));

        TextualReportDialog.showReport(this, report.toString(), "Import report");
    }

    private void appendToReport(StringBuilder report, List<ParameterSetDescriptor> descs) {
        if (descs == null || descs.isEmpty()) {
            report.append("NONE\n");
        } else {
            int maxNameLength = 0;
            for (ParameterSetDescriptor desc : descs) {
                int nameLength = desc.getName().length();
                if (nameLength > maxNameLength) {
                    maxNameLength = nameLength;
                }
            }

            for (ParameterSetDescriptor desc : descs) {
                report.append(StringUtils.pad(desc.getName(), maxNameLength + 5) + "["
                    + desc.shortClassName() + "]\n");
            }
        }
        report.append("\n\n");
    }

    public static List<String> selectParamSet(Window owner,
        List<ParameterSetDescriptor> dryRunResults) {

        ImportParamLibDialog dialog = new ImportParamLibDialog(owner, dryRunResults);
        dialog.setVisible(true);

        if (!dialog.cancelled) {
            return dialog.paramImportTableModel.getExcludeList();
        }
        return null;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(
            new ImportParamLibDialog(null, List.of(new ParameterSetDescriptor("name", "class"))));
    }

    private static class ParamLibImportTableModel
        extends AbstractZiggyTableModel<ParameterSetDescriptor> {

        private static final String[] COLUMN_NAMES = { "Include", "Parameter set name", "Class",
            "Action" };

        private List<ParameterSetDescriptor> paramMap = new LinkedList<>();
        private List<Boolean> includeFlags = new ArrayList<>();

        public ParamLibImportTableModel(List<ParameterSetDescriptor> paramMap) {
            this.paramMap = paramMap;

            // Include everything by default.
            includeFlags = new ArrayList<>();
            for (@SuppressWarnings("unused")
            ParameterSetDescriptor param : paramMap) {
                includeFlags.add(true);
            }
        }

        public List<String> getExcludeList() {
            List<String> excludeList = new LinkedList<>();

            for (int index = 0; index < paramMap.size(); index++) {
                if (!includeFlags.get(index)) {
                    excludeList.add(paramMap.get(index).getName());
                }
            }
            return excludeList;
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public int getRowCount() {
            return paramMap.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            // String name = names.get(rowIndex);
            boolean include = includeFlags.get(rowIndex);
            ParameterSetDescriptor param = paramMap.get(rowIndex);
            String className = param.shortClassName();

            switch (columnIndex) {
                case 0:
                    return include;
                case 1:
                    return param.getName();
                case 2:
                    return className;
                case 3:
                    State state = param.getState();
                    String color = "black";

                    switch (state) {
                        case CREATE:
                            color = "blue";
                            break;

                        case IGNORE:
                        case CLASS_MISSING:
                            color = "maroon";
                            break;

                        case SAME:
                            color = "green";
                            break;

                        case UPDATE:
                            color = "red";
                            break;

                        case LIBRARY_ONLY:
                            color = "purple";
                            break;

                        case NONE:
                            color = "black";
                            break;

                        default:
                    }

                    return htmlBuilder().appendBoldColor(state.toString(), color);
                default:
                    throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            }
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            if (columnIndex == 0) {
                return true;
            }
            return super.isCellEditable(rowIndex, columnIndex);
        }

        @Override
        public void setValueAt(Object value, int rowIndex, int columnIndex) {
            if (columnIndex != 0) {
                throw new IllegalArgumentException("read-only columnIndex = " + columnIndex);
            }
            Boolean newInclude = (Boolean) value;
            includeFlags.set(rowIndex, newInclude);
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            if (columnIndex == 0) {
                return Boolean.class;
            }
            return super.getColumnClass(columnIndex);
        }

        @Override
        public Class<ParameterSetDescriptor> tableModelContentClass() {
            return ParameterSetDescriptor.class;
        }

        @Override
        public ParameterSetDescriptor getContentAtRow(int row) {
            return paramMap.get(row);
        }
    }
}
