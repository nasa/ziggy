package gov.nasa.ziggy.ui.config.security;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class UserEditDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(UserEditDialog.class);

    private UserEditPanel userPanel;
    private final User user;
    private JButton cancelButton;
    private JButton saveButton;
    private JPanel buttonPanel;

    private final UserCrudProxy userCrud;

    public UserEditDialog(JFrame frame, User user) {
        super(frame, true);
        this.user = user;
        userCrud = new UserCrudProxy();
        initGUI();
    }

    public UserEditDialog(JFrame frame) {
        super(frame, true);
        user = new User();
        userCrud = new UserCrudProxy();
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            // START >> this
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            setTitle("Edit User " + user.getDisplayName());
            // END << this
            getContentPane().add(getUserPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            this.setSize(700, 483);
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private void saveButtonActionPerformed() {
        log.debug("saveButtonActionPerformed(ActionEvent) - start");
        try {
            userPanel.updateUser();
            userCrud.saveUser(user);
            setVisible(false);
        } catch (Exception e) {
            log.warn("caught e = ", e);
            JOptionPane.showMessageDialog(this, e, "Error Saving User", JOptionPane.ERROR_MESSAGE);
        }

        log.debug("saveButtonActionPerformed(ActionEvent) - end");
    }

    private void cancelButtonActionPerformed() {
        log.debug("cancelButtonActionPerformed(ActionEvent) - start");

        setVisible(false);

        log.debug("cancelButtonActionPerformed(ActionEvent) - end");
    }

    private UserEditPanel getUserPanel() {
        log.debug("getUserPanel() - start");

        if (userPanel == null) {
            userPanel = new UserEditPanel(user);
        }

        log.debug("getUserPanel() - end");
        return userPanel;
    }

    private JPanel getButtonPanel() {
        log.debug("getButtonPanel() - start");

        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }

        log.debug("getButtonPanel() - end");
        return buttonPanel;
    }

    private JButton getSaveButton() {
        log.debug("getSaveButton() - start");

        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("Save Changes");
            saveButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                saveButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getSaveButton() - end");
        return saveButton;
    }

    private JButton getCancelButton() {
        log.debug("getCancelButton() - start");

        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                cancelButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getCancelButton() - end");
        return cancelButton;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        UserEditDialog inst = new UserEditDialog(frame);
        inst.setVisible(true);

        log.debug("main(String[]) - end");
    }
}
