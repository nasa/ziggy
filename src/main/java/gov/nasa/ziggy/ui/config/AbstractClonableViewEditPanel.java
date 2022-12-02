package gov.nasa.ziggy.ui.config;

import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.common.MessageUtil;

@SuppressWarnings("serial")
public abstract class AbstractClonableViewEditPanel extends AbstractViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(AbstractClonableViewEditPanel.class);

    protected JMenuItem cloneMenuItem;
    protected JMenuItem renameMenuItem;

    private final boolean supportsClone;
    private final boolean supportsRename;

    public AbstractClonableViewEditPanel(boolean supportsClone, boolean supportsRename) {
        this.supportsClone = supportsClone;
        this.supportsRename = supportsRename;
    }

    protected abstract void doClone(int row);

    protected abstract String getCloneMenuText();

    protected abstract void doRename(int row);

    protected abstract String getRenameMenuText();

    private void cloneMenuItemActionPerformed() {
        try {
            doClone(selectedModelRow);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void renameMenuItemActionPerformed() {
        try {
            doRename(selectedModelRow);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void initGUI() throws PipelineUIException {
        super.initGUI();

        JPopupMenu menu = getPopupMenu();

        if (supportsClone) {
            menu.add(getCloneMenuItem());
        }

        if (supportsRename) {
            menu.add(getRenameMenuItem());
        }
    }

    protected JMenuItem getCloneMenuItem() {
        log.debug("getCloneMenuItem() - start");

        if (cloneMenuItem == null) {
            cloneMenuItem = new JMenuItem();
            cloneMenuItem.setText(getCloneMenuText());
            cloneMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                cloneMenuItemActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getCloneMenuItem() - end");
        return cloneMenuItem;
    }

    protected JMenuItem getRenameMenuItem() {
        log.debug("getRenameMenuItem() - start");

        if (renameMenuItem == null) {
            renameMenuItem = new JMenuItem();
            renameMenuItem.setText(getRenameMenuText());
            renameMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                renameMenuItemActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getRenameMenuItem() - end");
        return renameMenuItem;
    }
}
