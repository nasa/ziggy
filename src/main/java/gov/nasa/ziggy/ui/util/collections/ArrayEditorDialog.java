package gov.nasa.ziggy.ui.util.collections;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ADD_SYMBOL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.IMPORT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.OK;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REMOVE_SYMBOL;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;
import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.ui.util.FloatingPointTableCellRenderer;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;

/**
 * Dialog for editing the contents of a Java array.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ArrayEditorDialog extends javax.swing.JDialog {
    private JTable elementsTable;
    private JTextField addTextField;
    private ArrayEditorTableModel arrayEditorTableModel;
    private boolean cancelled;

    private Object array;

    public ArrayEditorDialog(Window owner, Object array) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.array = array;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Array editor");

        getContentPane().add(getDataPanel(), BorderLayout.CENTER);
        getContentPane().add(ZiggySwingUtils.createButtonPanel(createButton(OK, this::ok),
            createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        pack();
    }

    private JPanel getDataPanel() {
        arrayEditorTableModel = new ArrayEditorTableModel(array);
        elementsTable = new JTable(arrayEditorTableModel);
        elementsTable.getColumnModel().getColumn(1).setPreferredWidth(300);
        elementsTable.setDefaultRenderer(Float.class, new FloatingPointTableCellRenderer());
        elementsTable.setDefaultRenderer(Double.class, new FloatingPointTableCellRenderer());

        // Display the number of array elements, but no fewer than 8 rows and no more than 50 rows.
        int displayRows = Math.min(Math.max(Array.getLength(array), 8), 50);
        elementsTable.setPreferredScrollableViewportSize(
            new Dimension(350, displayRows * elementsTable.getRowHeight()));
        JScrollPane elementsScrollPane = new JScrollPane(elementsTable);

        JLabel addRemoveElement = ZiggySwingUtils.boldLabel("Add/remove element");
        JPanel addRemoveButtons = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(ADD_SYMBOL,
                "Insert the specified element before the selected row (or at the end if no row is selected)",
                this::addElement),
            createButton(REMOVE_SYMBOL, "Remove the element at the selected row", this::remove),
            createButton(IMPORT, this::importArray), createButton(EXPORT, this::exportArray));

        addTextField = new JTextField();
        addTextField.addActionListener(this::addElement);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(elementsScrollPane)
            .addComponent(addRemoveElement)
            .addComponent(addRemoveButtons)
            .addComponent(addTextField));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(elementsScrollPane)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(addRemoveElement)
            .addComponent(addRemoveButtons, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(addTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        return dataPanel;
    }

    private void addElement(ActionEvent evt) {
        int selectedIndex = elementsTable.getSelectedRow();

        if (selectedIndex == -1) {
            arrayEditorTableModel.insertElementAtEnd(addTextField.getText());
        } else {
            arrayEditorTableModel.insertElementAt(selectedIndex, addTextField.getText());
            elementsTable.getSelectionModel()
                .setSelectionInterval(selectedIndex + 1, selectedIndex + 1);
        }

        addTextField.setText("");
    }

    private void remove(ActionEvent evt) {
        int selectedIndex = elementsTable.getSelectedRow();

        if (selectedIndex != -1) {
            arrayEditorTableModel.removeElementAt(selectedIndex);

            int newSize = arrayEditorTableModel.getRowCount();
            if (selectedIndex < newSize) {
                elementsTable.getSelectionModel()
                    .setSelectionInterval(selectedIndex, selectedIndex);
            }
        }
    }

    private void importArray(ActionEvent evt) {
        try {
            JFileChooser fileChooser = new JFileChooser();
            int returnVal = fileChooser.showOpenDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fileChooser.getSelectedFile();

                List<String> newArray = ArrayImportExportUtils.importArray(file);
                arrayEditorTableModel.replaceWith(newArray);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void exportArray(ActionEvent evt) {
        try {
            JFileChooser fileChooser = new JFileChooser();
            int returnVal = fileChooser.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                File file = fileChooser.getSelectedFile();

                List<String> values = arrayEditorTableModel.asStringList();
                ArrayImportExportUtils.exportArray(file, values);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void ok(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    public Object editedArray() {
        return arrayEditorTableModel.asArray();
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public static void main(String[] args) {
        int size = 100;
        Integer[] array = new Integer[size];
        for (int i = 0; i < size; i++) {
            array[i] = i;
        }
        ZiggySwingUtils.displayTestDialog(new ArrayEditorDialog(null, array));
    }

    private static class ArrayEditorTableModel extends AbstractTableModel {

        private List<Object> elements = new LinkedList<>();

        private ZiggyDataType componentType;

        public ArrayEditorTableModel(Object array) {
            int length = Array.getLength(array);
            for (int i = 0; i < length; i++) {
                elements.add(Array.get(array, i));
            }

            componentType = ZiggyDataType.getDataType(array);
        }

        public Object asArray() {
            Object newArray = ZiggyArrayUtils.constructFullArray(new long[] { elements.size() },
                componentType, false);

            for (int index = 0; index < elements.size(); index++) {
                Array.set(newArray, index, elements.get(index));
            }

            return newArray;
        }

        public List<String> asStringList() {
            List<String> newList = new LinkedList<>();
            for (Object element : elements) {
                newList.add(ZiggyDataType.objectToString(element));
            }
            return newList;
        }

        public void replaceWith(List<String> newValues) {
            elements = new LinkedList<>();
            for (String newValue : newValues) {
                elements.add(componentType.typedValue(newValue));
            }

            fireTableDataChanged();
        }

        public void insertElementAt(int index, String text) {
            elements.add(index, componentType.typedValue(text));
            fireTableDataChanged();
        }

        public void insertElementAtEnd(String text) {
            insertElementAt(elements.size(), text);
        }

        public void removeElementAt(int selectedIndex) {
            elements.remove(selectedIndex);
            fireTableDataChanged();
        }

        @Override
        public int getColumnCount() {
            return 2;
        }

        @Override
        public int getRowCount() {
            return elements.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return switch (columnIndex) {
                case 0 -> rowIndex;
                case 1 -> elements.get(rowIndex);
                default -> throw new IllegalArgumentException(
                    "invalid columnIndex = " + columnIndex);
            };
        }

        @Override
        public void setValueAt(Object value, int rowIndex, int columnIndex) {
            if (columnIndex == 1) {
                elements.set(rowIndex, value);
            }
        }

        @Override
        public String getColumnName(int columnIndex) {
            return switch (columnIndex) {
                case 0 -> "Element";
                case 1 -> "Value";
                default -> throw new IllegalArgumentException(
                    "invalid columnIndex = " + columnIndex);
            };
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return columnIndex == 1;
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return switch (columnIndex) {
                case 0 -> Integer.class;
                case 1 -> componentType.getJavaBoxedClass();
                default -> throw new IllegalArgumentException(
                    "invalid columnIndex = " + columnIndex);
            };
        }
    }
}
