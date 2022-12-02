package gov.nasa.ziggy.ui.mon.master;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.netbeans.swing.outline.DefaultOutlineModel;
import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.OutlineModel;
import org.netbeans.swing.outline.RenderDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * {@link StatusPanel} for worker threads. Status information is displayed using {@link Outline}.
 *
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public class WorkerStatusPanel extends StatusPanel {
    private static final Logger log = LoggerFactory.getLogger(WorkerStatusPanel.class);

    private JScrollPane processesScrollPane;
    private JButton clearButton;
    private Outline processesOutline;
    private JPanel buttonPanel;

    private DefaultTreeModel treeModel;

    private OutlineModel outlineModel;
    private JButton collapseAllButton;
    private JButton expandAllButton;

    private Map<String, StatusNode> statusNodes = new HashMap<>();

    private DefaultMutableTreeNode rootTreeNode;

    public WorkerStatusPanel() {
        super();
        initGUI();
    }

    @Override
    public void update(StatusMessage statusMessage) {
        SwingUtilities.invokeLater(() -> {
            String key = statusMessage.uniqueKey();
            String processKey = statusMessage.getSourceProcess().getKey();
            StatusNode node = statusNodes.get(key);
            StatusNode processNode = statusNodes.get(processKey);

            // Add a "process node" if none present
            if (processNode == null) {
                processNode = new ProcessNode(statusMessage);
                DefaultMutableTreeNode parent = rootTreeNode;
                addNode(processKey, processNode, parent);
            }

            // Add a worker thread node if none present
            if (node == null) {
                StatusNode parentNode = statusNodes.get(processKey);
                StatusNode newNode = new WorkerThreadNode((WorkerStatusMessage) statusMessage);

                // add to owning process node
                DefaultMutableTreeNode parent = parentNode.getTreeNode();
                addNode(key, newNode, parent);
            } else {
                node.update(statusMessage);
                treeModel.nodeChanged(node.getTreeNode());
            }
            Indicator.State workersState = Indicator.State.GRAY;
            for (StatusNode statusNode : statusNodes.values()) {
                if (statusNode instanceof WorkerThreadNode) {
                    WorkerThreadNode workerNode = (WorkerThreadNode) statusNode;
                    if (!workerNode.getState().equals("IDLE")) {
                        workersState = Indicator.State.GREEN;
                        break;
                    }
                }
            }
            MasterStatusPanel.workersIndicator().setState(workersState);
            repaint();
        });
    }

    private void addNode(String key, StatusNode node, DefaultMutableTreeNode parent) {
        statusNodes.put(key, node);

        DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(node);
        node.setTreeNode(childNode);

        int index = parent.getChildCount();
        treeModel.insertNodeInto(childNode, parent, index);
        processesOutline.expandPath(new TreePath(childNode.getPath()));

    }

    private void clearButtonActionPerformed(ActionEvent evt) {
        log.debug("clearButton.actionPerformed, event=" + evt);

        statusNodes = new HashMap<>();

        rootTreeNode = new DefaultMutableTreeNode("");
        treeModel.setRoot(rootTreeNode);
    }

    private void expandAllButtonActionPerformed(ActionEvent evt) {
        log.debug("expandAllButton.actionPerformed, event=" + evt);

        int numKids = rootTreeNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootTreeNode.getChildAt(kidIndex);
            processesOutline.expandPath(new TreePath(kid.getPath()));
        }
    }

    private void collapseAllButtonActionPerformed(ActionEvent evt) {
        log.debug("collapseAllButton.actionPerformed, event=" + evt);

        int numKids = rootTreeNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootTreeNode.getChildAt(kidIndex);
            processesOutline.collapsePath(new TreePath(kid.getPath()));
        }
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            // setPreferredSize(new Dimension(400, 300));
            this.add(getProcessesScrollPane(), BorderLayout.CENTER);
            this.add(getButtonPanel(), BorderLayout.NORTH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getProcessesScrollPane() {
        if (processesScrollPane == null) {
            processesScrollPane = new JScrollPane();
            processesScrollPane.setViewportView(getProcessesOutline());
        }
        return processesScrollPane;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setAlignment(FlowLayout.LEFT);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getClearButton());
            buttonPanel.add(getExpandAllButton());
            buttonPanel.add(getCollapseAllButton());
        }
        return buttonPanel;
    }

    private JButton getClearButton() {
        if (clearButton == null) {
            clearButton = new JButton();
            clearButton.setText("clear");
            clearButton.addActionListener(this::clearButtonActionPerformed);
        }
        return clearButton;
    }

    private Outline getProcessesOutline() {
        if (processesOutline == null) {
            rootTreeNode = new DefaultMutableTreeNode("");
            treeModel = new DefaultTreeModel(rootTreeNode);

            outlineModel = DefaultOutlineModel.createOutlineModel(treeModel, new StatusRowModel(),
                false, "Worker Threads");

            processesOutline = new Outline();
            // if true, nothing is displayed
            // processesOutline.setRootVisible(false);
            processesOutline.setModel(outlineModel);
            processesOutline.setRenderDataProvider(new RenderData());
            processesOutline.expandPath(new TreePath(rootTreeNode.getPath()));
        }
        return processesOutline;
    }

    private class RenderData implements RenderDataProvider {
        @Override
        public java.awt.Color getBackground(Object o) {
            return null;
        }

        @Override
        public String getDisplayName(Object o) {
            DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) o;
            Object userObject = treeNode.getUserObject();

            if (userObject instanceof String) {
                return (String) userObject;
            } else if (userObject instanceof StatusNode) {
                StatusNode node = (StatusNode) userObject;
                return node.toString();
            } else {
                return "huh?";
            }
        }

        @Override
        public java.awt.Color getForeground(Object o) {
            Object node = ((DefaultMutableTreeNode) o).getUserObject();

            if (node instanceof WorkerThreadNode) {
                return Color.GRAY;
            }
            return null;
        }

        @Override
        public javax.swing.Icon getIcon(Object o) {
            // TODO: use age-colored balls
            return null;
        }

        @Override
        public String getTooltipText(Object o) {
            return "";
        }

        @Override
        public boolean isHtmlDisplayName(Object o) {
            return false;
        }
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new WorkerStatusPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private JButton getExpandAllButton() {
        if (expandAllButton == null) {
            expandAllButton = new JButton();
            expandAllButton.setText("+");
            expandAllButton.setToolTipText("Expand All");
            expandAllButton.addActionListener(this::expandAllButtonActionPerformed);
        }
        return expandAllButton;
    }

    private JButton getCollapseAllButton() {
        if (collapseAllButton == null) {
            collapseAllButton = new JButton();
            collapseAllButton.setText("-");
            collapseAllButton.setToolTipText("Collapse All");
            collapseAllButton.addActionListener(this::collapseAllButtonActionPerformed);
        }
        return collapseAllButton;
    }
}
