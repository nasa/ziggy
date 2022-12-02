package gov.nasa.ziggy.ui.config.general;

import javax.swing.JOptionPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.KeyValuePairCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractViewEditPanel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class KeyValuePairViewEditPanel extends AbstractViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(KeyValuePairViewEditPanel.class);

    private KeyValuePairTableModel keyValuePairTableModel; // do NOT init to null! (see
    // getTableModel)

    public KeyValuePairViewEditPanel() throws PipelineUIException {
        super();
        initGUI();
    }

    // $hide>>$
    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (keyValuePairTableModel == null) {
            keyValuePairTableModel = new KeyValuePairTableModel();
            keyValuePairTableModel.register();
        }

        log.debug("getTableModel() - end");
        return keyValuePairTableModel;
    }

    // $hide<<$

    @Override
    protected void doNew() {
        log.debug("doNew() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(new KeyValuePair());

        log.debug("doNew() - end");
    }

    @Override
    protected void doEdit(int row) {
        log.debug("doEdit(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(keyValuePairTableModel.getKeyValuePairAtRow(row));

        log.debug("doEdit(int) - end");
    }

    // $hide>>$
    @Override
    protected void doDelete(int row) {
        log.debug("doDelete(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        KeyValuePair keyValuePair = keyValuePairTableModel.getKeyValuePairAtRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete key '" + keyValuePair.getKey() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                // TODO: Need transaction/conversation context
                // TODO: only call create for new
                KeyValuePairCrud keyValuePairCrud = new KeyValuePairCrud();
                keyValuePairCrud.delete(keyValuePair);

                // ConfigurationOperations.deleteKeyValuePairTransacted( keyValuePair );
                keyValuePairTableModel.loadFromDatabase();
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            keyValuePairTableModel.loadFromDatabase();
        } catch (Exception e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(KeyValuePair keyValuePair) {
        log.debug("showEditDialog() - start");

        KeyValuePairEditDialog inst = ZiggyGuiConsole.newKeyValuePairEditDialog(keyValuePair);

        inst.setVisible(true);

        try {
            keyValuePairTableModel.loadFromDatabase();
        } catch (Exception e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
    }

    // $hide<<$

    @Override
    protected String getEditMenuText() {
        return "Edit selected key/value pair...";
    }

    @Override
    protected String getNewMenuText() {
        return "Add key/value pair...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected key/value pair...";
    }
}
