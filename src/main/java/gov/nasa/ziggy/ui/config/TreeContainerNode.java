package gov.nasa.ziggy.ui.config;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;

import gov.nasa.ziggy.ui.PipelineUIException;

/**
 * @author Todd Klaus
 */
public abstract class TreeContainerNode {
    protected String displayName = null;
    protected boolean loaded = false;

    public TreeContainerNode(String displayName) {
        this.displayName = displayName;
    }

    public void init(DefaultMutableTreeNode node) {
        DefaultMutableTreeNode loading = new DefaultMutableTreeNode("Loading...");

        node.add(loading);
    }

    public abstract void expand(DefaultTreeModel model, DefaultMutableTreeNode node)
        throws PipelineUIException;

    @Override
    public String toString() {
        return displayName;
    }

    protected void deleteAllChildren(DefaultTreeModel model, DefaultMutableTreeNode node) {
        for (int i = 0; i < node.getChildCount(); i++) {
            model.removeNodeFromParent((MutableTreeNode) node.getChildAt(i));
        }
    }
}
