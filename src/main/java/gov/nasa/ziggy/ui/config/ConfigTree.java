package gov.nasa.ziggy.ui.config;

import java.awt.Cursor;

import javax.swing.JTree;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.pipeline.PipelinesContainerNode;

/**
 * This class implements the navigation tree for the config panel.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ConfigTree extends JTree implements TreeSelectionListener, TreeExpansionListener {
    private static final Logger log = LoggerFactory.getLogger(ConfigTree.class);

    private DefaultTreeModel model = null;
    private ConfigDataPanel dataPanel = null;

    public enum TreeLabel {
        MODULE_LIBRARY("Module Library"),
        PARAMETER_LIBRARY("Parameter Library"),
        DATA_RECEIPT("Data Receipt"),
        EVENT_HANDLERS("Event Definitions"),
        PIPELINE_EVENTS("Pipeline Events"),
        DR_AVAILABLE_DATASETS("Available Datasets"),
        DR_DATA_ANOMALIES("Data Anomalies"),
        SECURITY("Security"),
        USERS("Users"),
        ROLES("Roles"),
        GENERAL("General");

        private String displayValue;

        TreeLabel(String displayValue) {
            this.displayValue = displayValue;
        }

        @Override
        public String toString() {
            return displayValue;
        }
    }

    public ConfigTree(ConfigDataPanel dataPanel) {
        super(initModel());

        this.dataPanel = dataPanel;
        model = (DefaultTreeModel) getModel();

        setRootVisible(false);
        putClientProperty("JTree.lineStyle", "Vertical");
        setShowsRootHandles(true);
        getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);

        addTreeSelectionListener(this);
        addTreeExpansionListener(this);
    }

    public void reloadModel() {
        setModel(initModel());
        model = (DefaultTreeModel) getModel();
        expandRow(3);
    }

    public static DefaultTreeModel initModel() {
        log.debug("initModel() - start");

        DefaultMutableTreeNode root = new DefaultMutableTreeNode("root");
        DefaultTreeModel model = new DefaultTreeModel(root);

        DefaultMutableTreeNode pipelines = new DefaultMutableTreeNode(new PipelinesContainerNode());
        root.add(pipelines);
        ((TreeContainerNode) pipelines.getUserObject()).init(pipelines);

        DefaultMutableTreeNode modules = new DefaultMutableTreeNode(TreeLabel.MODULE_LIBRARY);
        root.add(modules);

        DefaultMutableTreeNode moduleParamSets = new DefaultMutableTreeNode(
            TreeLabel.PARAMETER_LIBRARY);
        root.add(moduleParamSets);

        DefaultMutableTreeNode dataReceipt = new DefaultMutableTreeNode(TreeLabel.DATA_RECEIPT);
        root.add(dataReceipt);

        DefaultMutableTreeNode availableDatasets = new DefaultMutableTreeNode(
            TreeLabel.DR_AVAILABLE_DATASETS);
        dataReceipt.add(availableDatasets);

        DefaultMutableTreeNode dataAnomalies = new DefaultMutableTreeNode(
            TreeLabel.DR_DATA_ANOMALIES);
        dataReceipt.add(dataAnomalies);

        DefaultMutableTreeNode eventHandlers = new DefaultMutableTreeNode(TreeLabel.EVENT_HANDLERS);
        root.add(eventHandlers);

        DefaultMutableTreeNode events = new DefaultMutableTreeNode(TreeLabel.PIPELINE_EVENTS);
        root.add(events);

        DefaultMutableTreeNode security = new DefaultMutableTreeNode(TreeLabel.SECURITY);
        root.add(security);

        DefaultMutableTreeNode users = new DefaultMutableTreeNode(TreeLabel.USERS);
        security.add(users);

        DefaultMutableTreeNode roles = new DefaultMutableTreeNode(TreeLabel.ROLES);
        security.add(roles);

        DefaultMutableTreeNode general = new DefaultMutableTreeNode(TreeLabel.GENERAL);
        root.add(general);

        log.debug("initModel() - end");
        return model;
    }

    @Override
    public void valueChanged(TreeSelectionEvent event) {
        log.debug("valueChanged(TreeSelectionEvent) - start");

        log.debug("valueChanged, event=" + event);
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) getLastSelectedPathComponent();

        if (node == null) {
            return;
        }

        Object nodeInfo = node.getUserObject();
        if (node.isLeaf()) {
            log.debug("leaf = " + nodeInfo + ", class = " + nodeInfo.getClass());
        } else {
            log.debug("folder = " + nodeInfo + ", class = " + nodeInfo.getClass());
        }

        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        dataPanel.treeSelectionEvent(nodeInfo);
        setCursor(null);

        log.debug("valueChanged(TreeSelectionEvent) - end");
    }

    @Override
    public void treeExpanded(TreeExpansionEvent event) {
        log.debug("treeExpanded(TreeExpansionEvent) - start");

        log.debug("treeExpanded, event=" + event);
        TreePath path = event.getPath();
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
        log.debug("expanded node = " + node);

        Object userObject = node.getUserObject();
        if (userObject instanceof TreeContainerNode) {
            try {
                setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
                ((TreeContainerNode) userObject).expand(model, node);
                setCursor(null);
                scrollPathToVisible(
                    new TreePath(((DefaultMutableTreeNode) node.getFirstChild()).getPath()));
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }

        log.debug("treeExpanded(TreeExpansionEvent) - end");
    }

    @Override
    public void treeCollapsed(TreeExpansionEvent event) {
        log.debug("treeCollapsed(TreeExpansionEvent) - start");

        log.debug("treeCollapsed, event=" + event);

        log.debug("treeCollapsed(TreeExpansionEvent) - end");
    }
}
