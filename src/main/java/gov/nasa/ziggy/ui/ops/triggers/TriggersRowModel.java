package gov.nasa.ziggy.ui.ops.triggers;

import java.util.Date;

import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.RowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.User;

public class TriggersRowModel implements RowModel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TriggersRowModel.class);

    private TriggersTreeModel treeModel = null;

    public TriggersRowModel(TriggersTreeModel treeModel) {
        this.treeModel = treeModel;
    }

    @Override
    public int getColumnCount() {
        return 4;
    }

    @Override
    public boolean isCellEditable(Object node, int column) {
        return false;
    }

    @Override
    public void setValueFor(Object node, int column, Object value) {
        // not editable
    }

    @Override
    public Object getValueFor(Object treeNode, int columnIndex) {
        treeModel.validityCheck();
        Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();

        if (!(node instanceof PipelineDefinition)) {
            return "";
        }
        PipelineDefinition trigger = (PipelineDefinition) node;

        AuditInfo auditInfo = trigger.getAuditInfo();

        User lastChangedUser = null;
        Date lastChangedTime = null;

        if (auditInfo != null) {
            lastChangedUser = auditInfo.getLastChangedUser();
            lastChangedTime = auditInfo.getLastChangedTime();
        }

        switch (columnIndex) {
            case 0:
                return trigger.getName();
            case 1:
                if (lastChangedUser != null) {
                    return lastChangedUser.getLoginName();
                } else {
                    return "---";
                }
            case 2:
                if (lastChangedTime != null) {
                    return lastChangedTime;
                } else {
                    return "---";
                }
            case 3:
                return trigger.getNodes().size();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return String.class;
            case 1:
                return String.class;
            case 2:
                return Object.class;
            case 3:
                return Integer.class;
            default:
                return String.class;
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Name";
            case 1:
                return "User";
            case 2:
                return "Mod. Time";
            case 3:
                return "Node Count";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
