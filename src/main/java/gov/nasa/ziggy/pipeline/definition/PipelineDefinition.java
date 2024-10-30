package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ParameterSetReference;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * This class a pipeline configuration. A 'pipeline' is defined as a directed graph of
 * {@link PipelineDefinitionNode}s that share a common unit of work definition.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_PipelineDefinition",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "version" }) })
public class PipelineDefinition extends UniqueNameVersionPipelineComponent<PipelineDefinition> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinition.class);

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineDefinition_generator")
    @SequenceGenerator(name = "ziggy_PipelineDefinition_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineDefinition_sequence", allocationSize = 1)
    private Long id;

    @XmlAttribute(required = true)
    private String description;

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @XmlAttribute(required = false)
    @Enumerated(EnumType.STRING)
    @XmlJavaTypeAdapter(PipelineInstance.Priority.PriorityXmlAdapter.class)
    private Priority instancePriority = Priority.NORMAL;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_PipelineDefinition_parameterSetNames")
    private Set<String> parameterSetNames = new HashSet<>();

    // This needs the cascade types as shown so that, when a new version of PipelineDefinition
    // is created in the database, it points at all new versions of the nodes. That allows us
    // to rearrange or change the nodes in the new version without perturbing the old one.
    @ManyToMany(fetch = FetchType.EAGER)
    @Cascade({ CascadeType.PERSIST, CascadeType.MERGE, CascadeType.DETACH })
    @JoinTable(name = "ziggy_PipelineDefinition_rootNodes",
        joinColumns = { @JoinColumn(name = "id") },
        inverseJoinColumns = @JoinColumn(name = "childNode_id"))
    @OrderColumn(name = "idx")
    private List<PipelineDefinitionNode> rootNodes = new ArrayList<>();

    // Root node names are stored until post-processing can connect them to
    // the names of nodes in the pipeline.
    @XmlAttribute(required = true)
    @Transient
    private String rootNodeNames;

    // The following construction makes it possible to put the nodes and parameter sets for a
    // pipeline in any order in the XML, but it also complicates the bookkeeping required for those
    // objects.
    @XmlElements({ @XmlElement(name = "node", type = PipelineDefinitionNode.class),
        @XmlElement(name = "parameterSet", type = ParameterSetReference.class) })
    @Transient
    private Set<Object> nodesAndParamSets = new HashSet<>();

    public PipelineDefinition() {
    }

    public PipelineDefinition(String name) {
        setName(name);
    }

    /**
     * Constructs a renamed (deep) copy of this pipeline definition.
     *
     * @see UniqueNameVersionPipelineComponent#newInstance()
     */
    @Override
    public PipelineDefinition newInstance() {
        PipelineDefinition pipelineDefinition = super.newInstance();
        pipelineDefinition.id = null;

        pipelineDefinition.getParameterSetNames().addAll(parameterSetNames);

        pipelineDefinition.rootNodes = new ArrayList<>();
        for (PipelineDefinitionNode rootNode : rootNodes) {
            pipelineDefinition.rootNodes
                .add(new PipelineDefinitionNodeOperations().deepCopy(rootNode));
        }

        return pipelineDefinition;
    }

    public List<PipelineDefinitionNode> getRootNodes() {
        return rootNodes;
    }

    public void setRootNodes(List<PipelineDefinitionNode> rootNodes) {
        this.rootNodes = rootNodes;
        CollectionFilters.removeTypeFromCollection(nodesAndParamSets, PipelineDefinitionNode.class);
    }

    // Set the root node names from the rootNodes field.
    private void setRootNodeNames() {
        if (rootNodes.isEmpty()) {
            rootNodeNames = null;
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (PipelineDefinitionNode node : rootNodes) {
            sb.append(node.getModuleName());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        rootNodeNames = sb.toString();
    }

    public void addRootNode(PipelineDefinitionNode rootNode) {
        rootNodes.add(rootNode);
    }

    /**
     * Populates the fields that are used by the XML generator (specifically, the
     * {@link #nodesAndParamSets} and {@link #rootNodeNames} fields) from the other fields in the
     * instance (specifically, the ones that are obtained from a database retrieval). This ensures
     * that the database and XML versions of the instance's information are consistent with one
     * another at the moment of retrieval. The method also calls the
     * {@link PipelineDefinitionNode#populateXmlFields()} method for all nodes in the pipeline, thus
     * ensuring that they are also self-consistent.
     * <p>
     * Note: This method must be called whenever any of the set or add methods for nodes or
     * parameter sets are invoked. This ensures that the {@link #nodesAndParamSets} field remains
     * synchronized to the database fields that it mirrors.
     * <p>
     * Note: when any method sets the value of a database field that is mirrored in the
     * {@link #nodesAndParamSets} field, the {@link #nodesAndParamSets} field must have all
     * instances of the appropriate object class removed via the
     * {@link CollectionFilters#removeTypeFromCollection(java.util.Collection, Class)} static
     * method. This is necessary to ensure that (a) the {@link #nodesAndParamSets} field doesn't
     * keep any instances that are now obsolete due to the set operation, but (b) the instances of
     * all other classes in the {@link #nodesAndParamSets} field are not removed when the instances
     * from the given class are removed.
     */
    public void populateXmlFields() {
        List<PipelineDefinitionNode> allNodes = getNodes();
        nodesAndParamSets.addAll(allNodes);
        for (PipelineDefinitionNode node : allNodes) {
            node.populateXmlFields();
        }
        nodesAndParamSets.addAll(
            parameterSetNames.stream().map(ParameterSetReference::new).collect(Collectors.toSet()));

        // Don't touch the rootNodeNames String unless the rootNodes List is populated
        if (!rootNodes.isEmpty()) {
            setRootNodeNames();
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Long getId() {
        return id;
    }

    public Priority getInstancePriority() {
        return instancePriority;
    }

    public void setInstancePriority(Priority instancePriority) {
        this.instancePriority = instancePriority;
    }

    public Set<String> getParameterSetNames() {
        return parameterSetNames;
    }

    public void setParameterSetNames(Set<String> parameterSetNames) {
        this.parameterSetNames = parameterSetNames;
    }

    public void addParameterSetName(String parameterSetName) {
        parameterSetNames.add(parameterSetName);
    }

    // Obtains all the nodes by walking the tree from the root nodes.
    public List<PipelineDefinitionNode> getNodes() {
        List<PipelineDefinitionNode> pipelineDefinitionNodes = new ArrayList<>();
        addNodesToList(pipelineDefinitionNodes, rootNodes);
        return pipelineDefinitionNodes;
    }

    private void addNodesToList(List<PipelineDefinitionNode> pipelineDefinitionNodes,
        List<PipelineDefinitionNode> newNodes) {
        for (PipelineDefinitionNode definitionNode : newNodes) {
            if (pipelineDefinitionNodes.contains(definitionNode)) {
                throw new PipelineException(
                    "Circular reference: pipelineNodes Set already contains this node: "
                        + definitionNode);
            }
            pipelineDefinitionNodes.add(definitionNode);
            addNodesToList(pipelineDefinitionNodes, definitionNode.getNextNodes());
        }
    }

    public Set<PipelineDefinitionNode> nodesFromXml() {
        return CollectionFilters.filterToSet(nodesAndParamSets, PipelineDefinitionNode.class);
    }

    public Set<String> parameterSetNamesFromXml() {
        return CollectionFilters.filterToSet(nodesAndParamSets, ParameterSetReference.class)
            .stream()
            .map(ParameterSetReference::getName)
            .collect(Collectors.toSet());
    }

    public Set<Object> getNodesAndParamSets() {
        return nodesAndParamSets;
    }

    public String getRootNodeNames() {
        return rootNodeNames;
    }

    // Set the root nodes from a String.
    public void setRootNodeNames(String rootNodeNames) {
        this.rootNodeNames = rootNodeNames;
    }

    public void addPipelineInstances(Collection<PipelineInstance> pipelineInstances) {
        pipelineInstances.addAll(pipelineInstances);
    }

    /**
     * Defines {@link UniqueNameVersionPipelineComponent#totalEquals(Object)} for
     * {@link PipelineDefinition} instances. What this means in practice is the following:
     * <ol>
     * <li>the {@link AuditInfo} fields for the two instances are identical.
     * <li>The {@link Priority} fields for the two instances are identical.
     * <li>The {@link ParameterSet} names for the two instances are identical (note that the
     * instances themselves do not have to be identical, because we separately track changes to
     * parameter sets).
     * <li>The {@link PipelineDefinitionNode} graphs are identical for the two instances. Here we
     * mean that the node IDs are identical. The nodes are not name-version controlled because some
     * of the information in them is tracked separately (i.e., module parameter set name-and-version
     * are tracked for each pipeline task, so a change in the module parameters doesn't need to be
     * tracked in the node definition), and the rest is not relevant to data accountability (i.e.,
     * nobody cares that the user changed from remote to local execution).
     * </ol>
     */
    @Override
    public boolean totalEquals(Object other) {
        PipelineDefinition otherDef = (PipelineDefinition) other;

        if (!java.util.Objects.equals(description, otherDef.description)
            || !java.util.Objects.equals(instancePriority, otherDef.instancePriority)) {
            return false;
        }
        Set<String> otherParameterSetNames = otherDef.getParameterSetNames();
        if (otherParameterSetNames.size() != parameterSetNames.size()
            || !parameterSetNames.containsAll(otherParameterSetNames)
            || !nodeTreesIdentical(rootNodes, otherDef.rootNodes)) {
            return false;
        }
        return true;
    }

    /**
     * Recursive comparison of the node trees of two {@link PipelineDefinition}s.
     */
    private boolean nodeTreesIdentical(List<PipelineDefinitionNode> nodes,
        List<PipelineDefinitionNode> otherNodes) {

        // Check for empty-or-null condition so we know in subsequent steps that
        // neither list is null.
        if (CollectionUtils.isEmpty(nodes) && CollectionUtils.isEmpty(otherNodes)) {
            return true;
        }

        // Different sizes means false.
        if (nodes.size() != otherNodes.size()) {
            return false;
        }

        // Different nodes in the two lists (by ID number) means false.
        Map<Long, PipelineDefinitionNode> nodeMap = pipelineDefinitionNodeIds(nodes);
        Map<Long, PipelineDefinitionNode> otherNodeMap = pipelineDefinitionNodeIds(otherNodes);
        if (!nodeMap.keySet().containsAll(otherNodeMap.keySet())) {
            return false;
        }

        // At this point we know that the two instances contain the same nodes based on ID numbers.
        // Now we loop over this list's nodes, retrieve the matching node from the other list,
        // and compare their child nodes. This is where it gets recursive.
        boolean nextNodesIdentical = true;
        for (long id : nodeMap.keySet()) {
            List<PipelineDefinitionNode> nextNodes = nodeMap.get(id).getNextNodes();
            List<PipelineDefinitionNode> otherNextNodes = otherNodeMap.get(id).getNextNodes();
            if (CollectionUtils.isEmpty(nextNodes) && !CollectionUtils.isEmpty(otherNextNodes)
                || !CollectionUtils.isEmpty(nextNodes) && CollectionUtils.isEmpty(otherNextNodes)) {
                return false;
            }
            nextNodesIdentical = nextNodesIdentical
                && nodeTreesIdentical(nextNodes, otherNextNodes);
        }
        return nextNodesIdentical;
    }

    Map<Long, PipelineDefinitionNode> pipelineDefinitionNodeIds(
        List<PipelineDefinitionNode> nodes) {
        Map<Long, PipelineDefinitionNode> nodeMap = new HashMap<>();
        for (PipelineDefinitionNode node : nodes) {
            nodeMap.put(node.getId(), node);
        }
        return nodeMap;
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }
}
