package gov.nasa.ziggy.ui.security;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Window;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.proxy.UserCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class UserEditDialog extends javax.swing.JDialog {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(UserEditDialog.class);

    private EditUserPanel userPanel;
    private final User user;
    private JButton cancelButton;
    private JButton saveButton;
    private JPanel buttonPanel;

    private final UserCrudProxy userCrud;

    public UserEditDialog(Window owner, User user) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.user = user;
        userCrud = new UserCrudProxy();
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit User " + user.getDisplayName());
        getContentPane().setLayout(new BorderLayout());
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        getContentPane().add(getUserPanel(), BorderLayout.CENTER);
        getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
        setSize(700, 483);
    }

    private void saveButtonActionPerformed() {
        try {
            userPanel.updateUser();
            userCrud.saveUser(user);
            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, "Error Saving User", e.getMessage(), e);
        }
    }

    private void cancelButtonActionPerformed() {
        setVisible(false);
    }

    private EditUserPanel getUserPanel() {

        if (userPanel == null) {
            userPanel = new EditUserPanel(user);
        }

        return userPanel;
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
            saveButton.setText(SAVE);
            saveButton.addActionListener(evt -> {

                saveButtonActionPerformed();
            });
        }

        return saveButton;
    }

    private JButton getCancelButton() {

        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText(CANCEL);
            cancelButton.addActionListener(evt -> {

                cancelButtonActionPerformed();
            });
        }

        return cancelButton;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new UserEditDialog(null, new User()));
    }
}
