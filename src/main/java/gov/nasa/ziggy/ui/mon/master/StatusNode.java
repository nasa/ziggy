package gov.nasa.ziggy.ui.mon.master;

import javax.swing.tree.DefaultMutableTreeNode;

import gov.nasa.ziggy.services.process.StatusMessage;

public abstract class StatusNode {
    private DefaultMutableTreeNode treeNode = null;

    public abstract void update(StatusMessage statusMessage);

    public DefaultMutableTreeNode getTreeNode() {
        return treeNode;
    }

    public void setTreeNode(DefaultMutableTreeNode treeNode) {
        this.treeNode = treeNode;
    }
}
