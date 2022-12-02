package gov.nasa.ziggy.ui.mon.master;

import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.util.StringUtils;

public class StatusRowModel implements RowModel {
    @Override
    public Class<String> getColumnClass(int column) {
        return String.class;
    }

    @Override
    public int getColumnCount() {
        return 6;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "State";
            case 1:
                return "Age";
            case 2:
                return "Instance";
            case 3:
                return "Task";
            case 4:
                return "Module";
            case 5:
                return "UOW";
            default:
                return "huh?";
        }
    }

    @Override
    public Object getValueFor(Object treeNode, int column) {
        Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();
        if (node instanceof WorkerThreadNode) {
            WorkerThreadNode workerThreadNode = (WorkerThreadNode) node;
            switch (column) {
                case 0:
                    return workerThreadNode.getState();
                case 1:
                    return StringUtils.elapsedTime(workerThreadNode.getProcessingStartTime(),
                        System.currentTimeMillis());
                case 2:
                    return workerThreadNode.getInstanceId();
                case 3:
                    return workerThreadNode.getTaskId();
                case 4:
                    return workerThreadNode.getModule();
                case 5:
                    return workerThreadNode.getModuleUow();
                default:
                    break;
            }
        }
        return null;
    }

    @Override
    public boolean isCellEditable(Object node, int column) {
        return false;
    }

    @Override
    public void setValueFor(Object node, int column, Object value) {
    }
}
