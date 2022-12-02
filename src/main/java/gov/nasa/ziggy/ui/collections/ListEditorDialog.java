package gov.nasa.ziggy.ui.collections;

import java.awt.Component;
import java.awt.Dialog;
import java.awt.Frame;
import java.awt.GraphicsConfiguration;
import java.awt.Window;

import javax.swing.DefaultListModel;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionListener;

/**
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
class ListEditorDialog extends JDialog {
    private JList<Object> jList;

    public ListEditorDialog() {
        initUi();
    }

    /**
     * @param owner
     */
    public ListEditorDialog(Frame owner) {
        super(owner);
        initUi();
    }

    /**
     * @param owner
     */
    public ListEditorDialog(Dialog owner) {
        super(owner);
        initUi();
    }

    /**
     * @param owner
     */
    public ListEditorDialog(Window owner) {
        super(owner);
        initUi();
    }

    /**
     * @param owner
     * @param modal
     */
    public ListEditorDialog(Frame owner, boolean modal) {
        super(owner, modal);
        initUi();
    }

    /**
     * @param owner
     * @param title
     */
    public ListEditorDialog(Frame owner, String title) {
        super(owner, title);
        initUi();
    }

    /**
     * @param owner
     * @param modal
     */
    public ListEditorDialog(Dialog owner, boolean modal) {
        super(owner, modal);
        initUi();
    }

    /**
     * @param owner
     * @param title
     */
    public ListEditorDialog(Dialog owner, String title) {
        super(owner, title);
        initUi();
    }

    /**
     * @param owner
     * @param modalityType
     */
    public ListEditorDialog(Window owner, ModalityType modalityType) {
        super(owner, modalityType);
        initUi();
    }

    /**
     * @param owner
     * @param title
     */
    public ListEditorDialog(Window owner, String title) {
        super(owner, title);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modal
     */
    public ListEditorDialog(Frame owner, String title, boolean modal) {
        super(owner, title, modal);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modal
     */
    public ListEditorDialog(Dialog owner, String title, boolean modal) {
        super(owner, title, modal);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modalityType
     */
    public ListEditorDialog(Window owner, String title, ModalityType modalityType) {
        super(owner, title, modalityType);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modal
     * @param gc
     */
    public ListEditorDialog(Frame owner, String title, boolean modal, GraphicsConfiguration gc) {
        super(owner, title, modal, gc);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modal
     * @param gc
     */
    public ListEditorDialog(Dialog owner, String title, boolean modal, GraphicsConfiguration gc) {
        super(owner, title, modal, gc);
        initUi();
    }

    /**
     * @param owner
     * @param title
     * @param modalityType
     * @param gc
     */
    public ListEditorDialog(Window owner, String title, ModalityType modalityType,
        GraphicsConfiguration gc) {
        super(owner, title, modalityType, gc);
        initUi();
    }

    private void initUi() {
        jList = new JList<>();
        jList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        JScrollPane scrollPane = new JScrollPane(jList);
        add(scrollPane);
    }

    public void addListSelectionListener(ListSelectionListener listener) {
        jList.getSelectionModel().addListSelectionListener(listener);
    }

    public Object[] getSelectedValues() {
        return jList.getSelectedValuesList().toArray();
    }

    public void setSelectedIndices(int[] selectedIndices) {
        jList.setSelectedIndices(selectedIndices);
    }

    public void setAvailableValues(Object[] values) {
        DefaultListModel<Object> data = new DefaultListModel<>();
        for (Object value : values) {
            data.addElement(value);
        }
        jList.setModel(data);
    }

    public static ListEditorDialog newDialog(Component owner) {
        Window ownerWindow = SwingUtilities.windowForComponent(owner);

        ListEditorDialog dialog;
        if (ownerWindow instanceof JFrame) {
            dialog = new ListEditorDialog((JFrame) ownerWindow);
        } else {
            dialog = new ListEditorDialog((JDialog) ownerWindow);
        }
        dialog.setModalityType(ModalityType.APPLICATION_MODAL);
        return dialog;
    }
}
