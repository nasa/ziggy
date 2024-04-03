package gov.nasa.ziggy.ui.datastore;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreRegexpCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.ui.util.MessageUtil;

/**
 * Panel for editing the include and exclude regular expressions within a {@link DatastoreRegexp}.
 *
 * @author PT
 * @author Bill Wohler
 */
public class EditDatastoreRegexpDialog extends javax.swing.JDialog {

    private static final long serialVersionUID = 20240208L;

    private static final String TITLE = "Edit datastore regular expressions";

    private DatastoreRegexp datastoreRegexp;
    private boolean cancelled;
    private JTextField includeTextField;
    private JTextField excludeTextField;

    public EditDatastoreRegexpDialog(Window owner, DatastoreRegexp datastoreRegexp) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.datastoreRegexp = datastoreRegexp;
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle(TITLE);
        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(SAVE, this::save), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Name");
        JLabel nameText = new JLabel(datastoreRegexp.getName());
        JLabel value = boldLabel("Value");
        JLabel valueText = new JLabel(datastoreRegexp.getValue());
        JLabel include = boldLabel("Include");
        includeTextField = new JTextField(datastoreRegexp.getInclude());
        includeTextField.setColumns(minDialogWidth());
        JLabel exclude = boldLabel("Exclude");
        excludeTextField = new JTextField(datastoreRegexp.getExclude());
        excludeTextField.setColumns(minDialogWidth());

        JPanel panel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(panel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        panel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameText)
            .addComponent(value)
            .addComponent(valueText)
            .addComponent(include)
            .addComponent(includeTextField)
            .addComponent(exclude)
            .addComponent(excludeTextField));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(value)
            .addComponent(valueText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(include)
            .addComponent(includeTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(exclude)
            .addComponent(excludeTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        return panel;
    }

    /**
     * Returns the minimum dialog width to avoid truncating the title.
     *
     * @return the number of characters that a full-width field should use
     */
    private int minDialogWidth() {
        return TITLE.length();
    }

    private void save(ActionEvent evt) {
        try {
            // Trim the values. It's exceedingly unlikely that the data files have leading or
            // trailing spaces, but it's more likely that a user might accidently enter a space here
            // and then regexp would fail to match. If leading and trailing spaces are required,
            // then allow single or double quotes (' text ') to protect the space. Strip the quotes
            // before saving to the database, and add them if leading or trailing spaces are
            // detected when reading from the database.
            datastoreRegexp.setInclude(includeTextField.getText().trim());
            datastoreRegexp.setExclude(excludeTextField.getText().trim());
            DatabaseTransactionFactory.performTransaction(() -> {
                new DatastoreRegexpCrud().merge(datastoreRegexp);
                return null;
            });
            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
