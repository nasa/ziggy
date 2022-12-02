package gov.nasa.ziggy.ui.config;

import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.datareceipt.DataReceiptPanel;
import gov.nasa.ziggy.ui.config.events.ZiggyEventHandlerPanel;
import gov.nasa.ziggy.ui.config.events.ZiggyEventPanel;
import gov.nasa.ziggy.ui.config.general.KeyValuePairViewEditPanel;
import gov.nasa.ziggy.ui.config.module.ModuleLibraryViewEditPanel;
import gov.nasa.ziggy.ui.config.parameters.ParameterSetsViewEditPanel;
import gov.nasa.ziggy.ui.config.pipeline.PipelineGraphCanvas;
import gov.nasa.ziggy.ui.config.pipeline.PipelinesContainerNode;
import gov.nasa.ziggy.ui.config.pipeline.PipelinesViewEditPanel;
import gov.nasa.ziggy.ui.config.security.RolesViewEditPanel;
import gov.nasa.ziggy.ui.config.security.UsersViewEditPanel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ConfigDataPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(ConfigDataPanel.class);

    private static int PREFERRED_WIDTH = 400;
    private static int PREFERRED_HEIGHT = 300;

    private JPanel logoCard;
    private PipelinesViewEditPanel pipelinesPanel = null;
    private final Map<String, PipelineGraphCanvas> pipelineNodesPanelMap = new HashMap<>();
    private ModuleLibraryViewEditPanel moduleLibraryPanel = null;
    private ParameterSetsViewEditPanel moduleParamSetsPanel = null;
    private UsersViewEditPanel usersPanel = null;
    private RolesViewEditPanel rolesPanel = null;
    private KeyValuePairViewEditPanel keyValuePanel = null;
    private DataReceiptPanel dataReceiptPanel;
    private ZiggyEventHandlerPanel eventHandlerPanel;
    private ZiggyEventPanel eventPanel;

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        frame.getContentPane().add(new ConfigDataPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);

        log.debug("main(String[]) - end");
    }

    public ConfigDataPanel() {
        super();
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            CardLayout thisLayout = new CardLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(PREFERRED_WIDTH, PREFERRED_HEIGHT));
            this.add(getLogoCard(), "logoCard");
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private JPanel getLogoCard() {
        log.debug("getLogoCard() - start");

        if (logoCard == null) {
            logoCard = new ScalingImagePanel(ZiggyGuiConsole.pipelineOrZiggyImage());
            GridBagLayout logoCardLayout = new GridBagLayout();
            logoCard.setLayout(logoCardLayout);
            logoCard.setBackground(new java.awt.Color(255, 255, 255));
        }

        log.debug("getLogoCard() - end");
        return logoCard;
    }

    public void treeSelectionEvent(Object userObject) {
        log.debug("treeSelectionEvent(Object) - start");

        if (userObject instanceof ConfigTree.TreeLabel) {
            ConfigTree.TreeLabel selection = (ConfigTree.TreeLabel) userObject;
            log.debug("selection = " + selection);

            if (selection.equals(ConfigTree.TreeLabel.MODULE_LIBRARY)) {
                displayModuleLibraryPanel();
            } else if (selection.equals(ConfigTree.TreeLabel.PARAMETER_LIBRARY)) {
                displayModuleParamSetsPanel();
            } else if (selection.equals(ConfigTree.TreeLabel.USERS)) {
                displayUserPanel();
            } else if (selection.equals(ConfigTree.TreeLabel.ROLES)) {
                displayRolePanel();
            } else if (selection.equals(ConfigTree.TreeLabel.GENERAL)) {
                displayKeyValuePanel();
            } else if (selection.equals(ConfigTree.TreeLabel.DR_AVAILABLE_DATASETS)) {
                displayDataReceiptPanel();
            } else if (selection.equals(ConfigTree.TreeLabel.EVENT_HANDLERS)) {
                displayEventHandlers();
            } else if (selection.equals(ConfigTree.TreeLabel.PIPELINE_EVENTS)) {
                displayEvents();
            } else {
                ((CardLayout) getLayout()).show(this, "logoCard");
            }
        } else {
            log.debug("selection class = " + userObject.getClass());

            if (userObject instanceof PipelinesContainerNode) {
                displayPipelinePanel();
            } else if (userObject instanceof PipelineDefinition) {
                displayPipelineNodesPanel((PipelineDefinition) userObject);
            } else {
                ((CardLayout) getLayout()).show(this, "logoCard");
            }
        }

        log.debug("treeSelectionEvent(Object) - end");
    }

    private void displayEventHandlers() {
        if (eventHandlerPanel == null) {
            try {
                eventHandlerPanel = new ZiggyEventHandlerPanel();
                this.add(eventHandlerPanel, "eventHandlerCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "eventHandlerCard");
    }

    private void displayEvents() {
        if (eventPanel == null) {
            try {
                eventPanel = new ZiggyEventPanel();
                this.add(eventPanel, "eventCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "eventCard");
    }

    private void displayPipelinePanel() {
        if (pipelinesPanel == null) {
            try {
                pipelinesPanel = new PipelinesViewEditPanel();
                this.add(pipelinesPanel, "pipelinesCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "pipelinesCard");
    }

    private void displayDataReceiptPanel() {
        if (dataReceiptPanel == null) {
            dataReceiptPanel = new DataReceiptPanel();
            this.add(dataReceiptPanel, "dataReceiptCard");
        }
        ((CardLayout) getLayout()).show(this, "dataReceiptCard");
    }

    private void displayPipelineNodesPanel(PipelineDefinition pipeline) {
        String panelName = "pipelineNodesCard-" + pipeline.getId();
        PipelineGraphCanvas pipelineNodesPanel = pipelineNodesPanelMap.get(panelName);

        if (pipelineNodesPanel == null) {
            try {
                // pipelineNodesPanel = new PipelineNodesViewEditPanel( pipeline
                // );
                pipelineNodesPanel = new PipelineGraphCanvas(pipeline);
                this.add(pipelineNodesPanel, panelName);
                pipelineNodesPanelMap.put(panelName, pipelineNodesPanel);
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, panelName);
    }

    private void displayUserPanel() {
        if (usersPanel == null) {
            try {
                usersPanel = new UsersViewEditPanel();
                this.add(usersPanel, "usersCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "usersCard");
    }

    private void displayRolePanel() {
        if (rolesPanel == null) {
            try {
                rolesPanel = new RolesViewEditPanel();
                this.add(rolesPanel, "rolesCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "rolesCard");
    }

    private void displayModuleLibraryPanel() {
        if (moduleLibraryPanel == null) {
            try {
                moduleLibraryPanel = new ModuleLibraryViewEditPanel();
                this.add(moduleLibraryPanel, "moduleLibraryCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "moduleLibraryCard");
    }

    private void displayModuleParamSetsPanel() {
        if (moduleParamSetsPanel == null) {
            try {
                moduleParamSetsPanel = new ParameterSetsViewEditPanel();
                this.add(moduleParamSetsPanel, "moduleParamSetsCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "moduleParamSetsCard");
    }

    private void displayKeyValuePanel() {
        if (keyValuePanel == null) {
            try {
                keyValuePanel = new KeyValuePairViewEditPanel();
                this.add(keyValuePanel, "keyValueCard");
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
        ((CardLayout) getLayout()).show(this, "keyValueCard");
    }

    private static class ScalingImagePanel extends JPanel {

        private final Image image;

        public ScalingImagePanel(Image image) {
            super();
            this.image = image;
        }

        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            g.drawImage(image.getScaledInstance(getWidth(), getHeight(), Image.SCALE_SMOOTH), 0, 0,
                null);
        }
    }

}
