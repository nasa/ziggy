package gov.nasa.ziggy.ui.common;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class DoubleListDialog<T> extends javax.swing.JDialog {
    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JButton cancelButton;
    private JButton saveButton;

    private boolean savePressed = false;
    private JButton removeButton;
    private JLabel availListLabel;
    private JButton addButton;
    private JList<T> selectedList;
    private JList<T> availList;
    private JScrollPane selectedScrollPane;
    private JScrollPane availScrollPane;
    private JLabel selectedListLabel;
    private boolean cancelPressed = false;
    private String availableListTitle = "Available";
    private GenericListModel<T> availableListModel = new GenericListModel<>();
    private String selectedListTitle = "Selected";
    private GenericListModel<T> selectedListModel = new GenericListModel<>();

    public DoubleListDialog(JFrame frame) {
        super(frame, "Select", true);
        initGUI();
    }

    public DoubleListDialog(JFrame frame, String title, String availableListTitle,
        List<T> availableListContents, String selectedListTitle, List<T> selectedListContents) {
        super(frame, title, true);
        this.availableListTitle = availableListTitle;
        this.availableListModel = new GenericListModel<>(availableListContents);
        this.selectedListTitle = selectedListTitle;
        this.selectedListModel = new GenericListModel<>(selectedListContents);
        initGUI();
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            setSize(400, 300);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getAvailListLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getSelectedListLabel(),
                new GridBagConstraints(6, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getAvailScrollPane(),
                new GridBagConstraints(0, 1, 3, 3, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getSelectedScrollPane(),
                new GridBagConstraints(6, 1, 3, 3, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getAddButton(), new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getRemoveButton(), new GridBagConstraints(4, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }
        return buttonPanel;
    }

    private JButton getSaveButton() {
        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("Save Changes");
            saveButton.addActionListener(evt -> saveButtonActionPerformed());
        }
        return saveButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(evt -> cancelButtonActionPerformed());
        }
        return cancelButton;
    }

    private void saveButtonActionPerformed() {
        savePressed = true;
        setVisible(false);
    }

    private void cancelButtonActionPerformed() {
        cancelPressed = true;
        setVisible(false);
    }

    /**
     * @return Returns the cancelPressed.
     */
    public boolean wasCancelPressed() {
        return cancelPressed;
    }

    /**
     * @return Returns the savePressed.
     */
    public boolean wasSavePressed() {
        return savePressed;
    }

    private JLabel getAvailListLabel() {
        if (availListLabel == null) {
            availListLabel = new JLabel();
            availListLabel.setText(availableListTitle);
        }
        return availListLabel;
    }

    private JLabel getSelectedListLabel() {
        if (selectedListLabel == null) {
            selectedListLabel = new JLabel();
            selectedListLabel.setText(selectedListTitle);
        }
        return selectedListLabel;
    }

    private JScrollPane getAvailScrollPane() {
        if (availScrollPane == null) {
            availScrollPane = new JScrollPane();
            availScrollPane.setViewportView(getAvailList());
        }
        return availScrollPane;
    }

    private JScrollPane getSelectedScrollPane() {
        if (selectedScrollPane == null) {
            selectedScrollPane = new JScrollPane();
            selectedScrollPane.setViewportView(getSelectedList());
        }
        return selectedScrollPane;
    }

    private JList<T> getAvailList() {
        if (availList == null) {
            availList = new JList<>();
            availList.setModel(availableListModel);
        }
        return availList;
    }

    private JList<T> getSelectedList() {
        if (selectedList == null) {
            selectedList = new JList<>();
            selectedList.setModel(selectedListModel);
        }
        return selectedList;
    }

    private JButton getAddButton() {
        if (addButton == null) {
            addButton = new JButton();
            addButton.setText("->");
            addButton.addActionListener(evt -> addButtonActionPerformed());
        }
        return addButton;
    }

    private JButton getRemoveButton() {
        if (removeButton == null) {
            removeButton = new JButton();
            removeButton.setText("<-");
            removeButton.addActionListener(evt -> removeButtonActionPerformed());
        }
        return removeButton;
    }

    private void addButtonActionPerformed() {
        int availIndex = availList.getSelectedIndex();

        if (availIndex == -1) {
            return;
        }

        selectedListModel.add(availableListModel.remove(availIndex));
    }

    private void removeButtonActionPerformed() {
        int selectedIndex = selectedList.getSelectedIndex();

        if (selectedIndex == -1) {
            return;
        }

        availableListModel.add(selectedListModel.remove(selectedIndex));
    }

    /**
     * @return Returns the availableListModel.
     */
    public List<T> getAvailableListContents() {
        return availableListModel.getList();
    }

    /**
     * @return Returns the selectedListModel.
     */
    public List<T> getSelectedListContents() {
        return selectedListModel.getList();
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        List<String> a = new LinkedList<>();
        a.add("A");
        a.add("B");
        a.add("C");
        List<String> s = new LinkedList<>();

        DoubleListDialog<String> inst = new DoubleListDialog<>(frame, "Select some letters",
            "Available letters", a, "Selected letters", s);
        inst.setVisible(true);
    }
}
