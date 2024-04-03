package gov.nasa.ziggy.data.datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.crud.AbstractCrud;

public class DatastoreNodeCrud extends AbstractCrud<DatastoreNode> {

    public Map<String, DatastoreNode> retrieveNodesByFullPath() {
        Map<String, DatastoreNode> nodesByFullPath = new HashMap<>();
        List<DatastoreNode> nodes = list(createZiggyQuery(DatastoreNode.class));
        for (DatastoreNode node : nodes) {
            nodesByFullPath.put(node.getFullPath(), node);
        }
        return nodesByFullPath;
    }

    @Override
    public Class<DatastoreNode> componentClass() {
        return DatastoreNode.class;
    }
}
