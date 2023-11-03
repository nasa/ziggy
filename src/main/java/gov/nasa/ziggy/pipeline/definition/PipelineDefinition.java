package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ParameterSetReference;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
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
public class PipelineDefinition extends UniqueNameVersionPipelineComponent<PipelineDefinition>
    implements HasGroup {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinition.class);

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineDefinition_generator")
    @SequenceGenerator(name = "ziggy_PipelineDefinition_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineDefinition_sequence", allocationSize = 1)
    private Long id;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    @XmlAttribute(required = true)
    private String description;

    @ManyToOne
    private Group group;

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @XmlAttribute(required = false)
    @Enumerated(EnumType.STRING)
    @XmlJavaTypeAdapter(PipelineInstance.Priority.PriorityXmlAdapter.class)
    private Priority instancePriority = Priority.NORMAL;

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineDefinition_pipelineParameterSetNames")
    private Map<ClassWrapper<ParametersInterface>, String> pipelineParameterSetNames = new HashMap<>();

    @ManyToMany
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
        @XmlElement(name = "pipelineParameter", type = ParameterSetReference.class) })
    @Transient
    private Set<Object> nodesAndParamSets = new HashSet<>();

    public PipelineDefinition() {
    }

    public PipelineDefinition(String name) {
        setName(name);
    }

    public PipelineDefinition(AuditInfo auditInfo, String name) {
        setName(name);
        this.auditInfo = auditInfo;
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
        if (auditInfo != null) {
            pipelineDefinition.auditInfo = new AuditInfo(auditInfo.getLastChangedUser(),
                auditInfo.getLastChangedTime());
        }
        if (group != null) {
            pipelineDefinition.group = new Group(group);
        }

        pipelineDefinition.pipelineParameterSetNames = new HashMap<>();
        for (Entry<ClassWrapper<ParametersInterface>, String> pipelineParameterSetName : pipelineParameterSetNames
            .entrySet()) {
            pipelineDefinition.pipelineParameterSetNames.put(pipelineParameterSetName.getKey(),
                pipelineParameterSetName.getValue());
        }

        pipelineDefinition.rootNodes = new ArrayList<>();
        for (PipelineDefinitionNode rootNode : rootNodes) {
            pipelineDefinition.rootNodes.add(new PipelineDefinitionNode(rootNode));
        }

        return pipelineDefinition;
    }

    /**
     * Walks the tree of {@link PipelineDefinitionNode}s for this pipeline definition and sets the
     * parentNode and path fields for each one.
     */
    public void buildPaths() {
        Stack<Integer> path = new Stack<>();
        buildPaths(null, rootNodes, path);
    }

    /**
     * Recursive method to set parent pointers
     *
     * @param parent
     * @param kids
     * @param path
     */
    private void buildPaths(PipelineDefinitionNode parent, List<PipelineDefinitionNode> kids,
        Stack<Integer> path) {
        for (int i = 0; i < kids.size(); i++) {
            PipelineDefinitionNode kid = kids.get(i);
            path.push(i);

            kid.setParentNode(parent);
            kid.setPath(new PipelineDefinitionNodePath(new ArrayList<>(path)));

            buildPaths(kid, kid.getNextNodes(), path);

            path.pop();
        }
    }

    public List<PipelineDefinitionNode> getRootNodes() {
        return rootNodes;
    }

    public void setRootNodes(List<PipelineDefinitionNode> rootNodes) {
        this.rootNodes = rootNodes;
        CollectionFilters.removeTypeFromCollection(nodesAndParamSets, PipelineDefinitionNode.class);
        populateXmlFields();
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
        populateXmlFields();
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
        nodesAndParamSets.addAll(pipelineParameterSetNames.values()
            .stream()
            .map(ParameterSetReference::new)
            .collect(Collectors.toSet()));

        // Don't touch the rootNodeNames String unless the rootNodes List is populated
        if (!rootNodes.isEmpty()) {
            setRootNodeNames();
        }
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void setAuditInfo(AuditInfo auditInfo) {
        this.auditInfo = auditInfo;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Group group() {
        return group;
    }

    @Override
    public void setGroup(Group group) {
        this.group = group;
    }

    public Long getId() {
        return id;
    }

    public String getGroupName() {
        Group group = group();
        if (group == null) {
            return Group.DEFAULT.getName();
        }
        return group.getName();
    }

    public Priority getInstancePriority() {
        return instancePriority;
    }

    public void setInstancePriority(Priority instancePriority) {
        this.instancePriority = instancePriority;
    }

    public Map<ClassWrapper<ParametersInterface>, String> getPipelineParameterSetNames() {
        return pipelineParameterSetNames;
    }

    // Populates the Map from Parameter class to ParameterSetName.
    public void setPipelineParameterSetNames(
        Map<ClassWrapper<ParametersInterface>, String> pipelineParameterSetNames) {
        this.pipelineParameterSetNames = pipelineParameterSetNames;
        CollectionFilters.removeTypeFromCollection(nodesAndParamSets, String.class);
        populateXmlFields();
    }

    // Adds an element to the Map from Parameter class to ParameterSetName.
    public void addPipelineParameterSetName(Class<? extends ParametersInterface> parameterClass,
        ParameterSet parameterSet) {
        ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(parameterClass);
        if (pipelineParameterSetNames.containsKey(classWrapper)) {
            throw new PipelineException(
                "This PipelineDefinition already contains a pipeline parameter set for class: "
                    + parameterClass);
        }
        pipelineParameterSetNames.put(classWrapper, parameterSet.getName());
        populateXmlFields();
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

    public PipelineDefinitionNode getNodeByName(String name) {
        if (name == null) {
            return null;
        }
        List<PipelineDefinitionNode> nodes = getNodes();
        List<PipelineDefinitionNode> nodeNameMatches = nodes.stream()
            .filter(s -> name.equals(s.getModuleName()))
            .collect(Collectors.toList());
        PipelineDefinitionNode matchedNode = null;
        if (!nodeNameMatches.isEmpty()) {
            matchedNode = nodeNameMatches.get(0);
        }
        return matchedNode;
    }

    public Set<PipelineDefinitionNode> getNodesFromXml() {
        return CollectionFilters.filterToSet(nodesAndParamSets, PipelineDefinitionNode.class);
    }

    public Set<String> getParameterSetNames() {
        return CollectionFilters.filterToSet(nodesAndParamSets, ParameterSetReference.class)
            .stream()
            .map(ParameterSetReference::getName)
            .collect(Collectors.toSet());
    }

    public String getRootNodeNames() {
        return rootNodeNames;
    }

    // Set the root nodes from a String.
    public void setRootNodeNames(String rootNodeNames) {
        this.rootNodeNames = rootNodeNames;
    }

    // TODO: Define what totalEquals() should do in the case of a pipeline definition.
    @Override
    public boolean totalEquals(Object other) {
        return false;
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }
}
