package gov.nasa.ziggy.ui.util.collections;

import java.awt.Component;
import java.awt.Window;

import javax.swing.DefaultListModel;
import javax.swing.JDialog;
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

    public ListEditorDialog(Window owner) {
        super(owner);
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
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
        return new ListEditorDialog(SwingUtilities.getWindowAncestor(owner));
    }
}
