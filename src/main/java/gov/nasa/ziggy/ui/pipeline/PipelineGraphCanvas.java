package gov.nasa.ziggy.ui.pipeline;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.ui.util.MessageUtils;

/**
 * Display the pipeline nodes as a directed graph with pop-up menus on each node.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineGraphCanvas extends JPanel {
    private static final Logger log = LoggerFactory.getLogger(PipelineGraphCanvas.class);

    private enum DrawType {
        LINES, NODES
    }

    // size of the canvas (TODO: make dynamic based on # nodes)
    private static final int CANVAS_HEIGHT = 750;
    private static final int CANVAS_WIDTH = 500;

    // spacing at the top and left of the canvas
    private final static int HORIZONTAL_INSET = 20;
    private final static int VERTICAL_INSET = 20;

    // spacing between rows & columns of widgets
    private final static int ROW_SPACING = 100;
    private final static int COLUMN_SPACING = 220;

    // widget sizes
    private final static int START_WIDGET_WIDTH = 100;
    private final static int START_WIDGET_HEIGHT = 30;
    private final static int NODE_WIDGET_WIDTH = 250;
    private final static int NODE_WIDGET_HEIGHT = 30;

    private final PipelineDefinition pipeline;
    private PipelineNodeWidget selectedNodeWidget;

    private JPopupMenu canvasPopupMenu;
    private JPanel canvasPane;
    private JScrollPane scrollPane;
    private JMenuItem insertBeforeMenuItem;
    private JMenuItem insertAfterMenuItem;
    private JMenuItem insertBranchMenuItem;
    protected JMenuItem editMenuItem;
    protected JMenuItem deleteMenuItem;

    private final PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private final PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();

    public PipelineGraphCanvas(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
        buildComponent();
    }

    private void buildComponent() {
        BorderLayout layout = new BorderLayout();
        setLayout(layout);
        scrollPane = getScrollPane();
        add(scrollPane, BorderLayout.CENTER);

        // TODO Fix or delete menu
        // Commands currently throw NullPointerException, so hide for now.
        // setComponentPopupMenu(canvasPane, getCanvasPopupMenu());
    }

    public void redraw() {
        canvasPane.removeAll();
        drawPipeline(pipeline, DrawType.NODES, null);
        revalidate();
        repaint();
    }

    private void drawPipeline(PipelineDefinition pipeline, DrawType drawType, Graphics2D g2) {
        int horzInset = HORIZONTAL_INSET + (NODE_WIDGET_WIDTH - START_WIDGET_WIDTH) / 2;
        Rectangle nodeBounds = new Rectangle(horzInset, VERTICAL_INSET, START_WIDGET_WIDTH,
            START_WIDGET_HEIGHT);

        if (drawType == DrawType.NODES) {
            PipelineNodeWidget widget = new PipelineNodeWidget(pipeline);
            canvasPane.add(widget);
            widget.setBounds(nodeBounds);
        }

        int childColumn = 0;
        for (PipelineDefinitionNode rootNode : pipeline.getRootNodes()) {
            drawNodes(rootNode, null, 1, childColumn++, drawType, nodeBounds, g2);
        }
    }

    private void drawNodes(PipelineDefinitionNode node, PipelineDefinitionNode parentNode, int row,
        int column, DrawType drawType, Rectangle parentBounds, Graphics2D g2) {

        Rectangle nodeBounds = new Rectangle(HORIZONTAL_INSET + COLUMN_SPACING * column,
            VERTICAL_INSET + ROW_SPACING * row, NODE_WIDGET_WIDTH, NODE_WIDGET_HEIGHT);

        if (drawType == DrawType.NODES) {
            PipelineNodeWidget widget = new PipelineNodeWidget(node, parentNode);
            canvasPane.add(widget);
            widget.setBounds(nodeBounds);
        } else if (parentBounds != null) {
            drawArrow(g2, parentBounds, nodeBounds);
        }

        int childColumn = column;

        for (PipelineDefinitionNode childNode : node.getNextNodes()) {
            drawNodes(childNode, node, row + 1, childColumn, drawType, nodeBounds, g2);
            childColumn++;
        }
    }

    private void drawArrow(Graphics2D g2, Rectangle start, Rectangle end) {

        Stroke oldStroke = g2.getStroke();

        BasicStroke lineStroke = new BasicStroke(2);

        g2.setStroke(lineStroke);

        int startX = start.x + start.width / 2;
        int startY = start.y + start.height;
        int endX = end.x + end.width / 2;
        int endY = end.y;

        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        g2.drawLine(startX, startY, endX, endY);

        double relativeX = startX - endX;
        double relativeY = endY - startY;

        double lineAngle = Math
            .acos(relativeX / Math.sqrt(relativeX * relativeX + relativeY * relativeY));
        double nearArrowheadAngle = lineAngle - Math.PI / 4.0;
        double farArrowheadAngle = lineAngle + Math.PI / 4.0;

        int arrowheadLength = 12;
        int nearArrowheadEndpointX = (int) (arrowheadLength * Math.cos(nearArrowheadAngle));
        int nearArrowheadEndpointY = (int) (arrowheadLength * Math.sin(nearArrowheadAngle));

        int farArrowheadEndpointX = (int) (arrowheadLength * Math.cos(farArrowheadAngle));
        int farArrowheadEndpointY = (int) (arrowheadLength * Math.sin(farArrowheadAngle));

        Polygon arrowHead = new Polygon();
        arrowHead.addPoint(endX, endY);
        arrowHead.addPoint(endX + nearArrowheadEndpointX, endY - nearArrowheadEndpointY);
        arrowHead.addPoint(endX + farArrowheadEndpointX, endY - farArrowheadEndpointY);

        g2.fillPolygon(arrowHead);

        g2.setStroke(oldStroke);
    }

    private void showEditDialog(PipelineDefinitionNode pipelineNode) {

        try {
            EditPipelineNodeDialog dialog = new EditPipelineNodeDialog(
                SwingUtilities.getWindowAncestor(this), pipeline, pipelineNode);
            dialog.setVisible(true);

            if (dialog.wasSavePressed()) {
                pipelineDefinitionOperations().merge(pipeline);
                redraw();
            }
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    private void enableMenuItemsPerContext(PipelineNodeWidget selectedNodeWidget) {
        if (pipeline.isLocked()) {
            insertAfterMenuItem.setEnabled(false);
            insertBranchMenuItem.setEnabled(false);
            insertBeforeMenuItem.setEnabled(false);
            // editMenuItem.setEnabled(false);
            deleteMenuItem.setEnabled(false);
        } else if (selectedNodeWidget.isStartNode()) {
            insertBeforeMenuItem.setEnabled(false);
            editMenuItem.setEnabled(false);
            deleteMenuItem.setEnabled(false);
        } else {
            insertBeforeMenuItem.setEnabled(true);
            editMenuItem.setEnabled(true);
            deleteMenuItem.setEnabled(true);
        }
    }

    private void edit(ActionEvent evt) {
        showEditDialog(selectedNodeWidget.getPipelineNode());
    }

    private void delete(ActionEvent evt) {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode parentNode = selectedNodeWidget.getPipelineNodeParent();

            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete pipelineNode '" + selectedNode.getId() + "'?");

            if (choice == JOptionPane.YES_OPTION) {
                List<PipelineDefinitionNode> currentNextNodes = null;

                if (parentNode != null) {
                    currentNextNodes = parentNode.getNextNodes();
                } else {
                    currentNextNodes = pipeline.getRootNodes();
                }

                // remove selected node from parent's nextNodes list
                currentNextNodes.remove(selectedNode);

                // add selected node's nextNodes to parent's nextNodes list
                currentNextNodes.addAll(selectedNode.getNextNodes());

                pipelineDefinitionNodeOperations().delete(selectedNode);

                redraw();
            }
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    private void insertBefore(ActionEvent evt) {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode predecessorNode = selectedNodeWidget.getPipelineNodeParent();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();

            EditPipelineNodeDialog editPipelineNodeDialog = new EditPipelineNodeDialog(
                SwingUtilities.getWindowAncestor(this), pipeline, newNode);
            editPipelineNodeDialog.setVisible(true);

            if (editPipelineNodeDialog.wasSavePressed()) {
                List<PipelineDefinitionNode> predecessorsNextNodesList = null;
                if (predecessorNode != null) {
                    predecessorsNextNodesList = predecessorNode.getNextNodes();
                } else {
                    // inserting the first node
                    predecessorsNextNodesList = pipeline.getRootNodes();
                }

                // change predecessor's nextNodes entry for this node to new
                // node
                int selectedNodeIndex = predecessorsNextNodesList.indexOf(selectedNode);
                assert selectedNodeIndex != -1 : "PipelineGraphCanvas:doInsertBefore: predecessor node does not point to this node!";

                predecessorsNextNodesList.set(selectedNodeIndex, newNode);

                // set new node's nextNodes to this node
                newNode.getNextNodes().add(selectedNode);

                pipelineDefinitionOperations().merge(pipeline);
                redraw();
            }
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    private void insertAfter(ActionEvent evt) {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();

            EditPipelineNodeDialog editPipelineNodeDialog = new EditPipelineNodeDialog(
                SwingUtilities.getWindowAncestor(this), pipeline, newNode);
            editPipelineNodeDialog.setVisible(true);

            if (editPipelineNodeDialog.wasSavePressed()) {
                List<PipelineDefinitionNode> currentNextNodes = null;

                if (selectedNodeWidget.isStartNode()) {
                    currentNextNodes = selectedNodeWidget.getPipeline().getRootNodes();
                } else {
                    currentNextNodes = selectedNode.getNextNodes();
                }

                // copy selected node's nextNodes to new node's nextNodes
                List<PipelineDefinitionNode> newNodeNextNodes = new ArrayList<>(currentNextNodes);
                newNode.setNextNodes(newNodeNextNodes);

                // set selected node's nextNodes to new node
                List<PipelineDefinitionNode> selectedNodeNextNodes = currentNextNodes;
                selectedNodeNextNodes.clear();
                selectedNodeNextNodes.add(newNode);

                pipelineDefinitionOperations().merge(pipeline);
                redraw();
            }
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    private void insertBranch(ActionEvent evt) {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();

            EditPipelineNodeDialog editPipelineNodeDialog = new EditPipelineNodeDialog(
                SwingUtilities.getWindowAncestor(this), pipeline, newNode);
            editPipelineNodeDialog.setVisible(true);

            if (editPipelineNodeDialog.wasSavePressed()) {
                List<PipelineDefinitionNode> currentNextNodes = null;

                if (selectedNodeWidget.isStartNode()) {
                    currentNextNodes = selectedNodeWidget.getPipeline().getRootNodes();
                } else {
                    currentNextNodes = selectedNode.getNextNodes();
                }

                // add new node to selected node's nextNodes
                currentNextNodes.add(newNode);

                pipelineDefinitionOperations().merge(pipeline);
                redraw();
            }
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    private void canvasPaneMouseClicked(MouseEvent evt) {
        if (evt.getClickCount() == 2) {
            Component selectedComponent = canvasPane.getComponentAt(evt.getX(), evt.getY());
            log.debug("MP:selectedComponent = " + selectedComponent);
            if (selectedComponent instanceof PipelineNodeWidget) {
                selectedNodeWidget = (PipelineNodeWidget) selectedComponent;

                // TODO Fix or delete command
                // Command currently throws NullPointerException, so suppress for now.
                // doEdit();
            }
        }
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getCanvasPane());
        }

        return scrollPane;
    }

    private JPanel getCanvasPane() {
        if (canvasPane == null) {
            canvasPane = new JPanel() {
                @Override
                protected void paintComponent(Graphics g) {
                    super.paintComponent(g);
                    // TODO: currently only supports one root node
                    drawPipeline(pipeline, DrawType.LINES, (Graphics2D) g);
                }
            };
            canvasPane.setBackground(new java.awt.Color(255, 255, 255));
            canvasPane.setLayout(null);
            canvasPane.setPreferredSize(new Dimension(CANVAS_WIDTH, CANVAS_HEIGHT));
            canvasPane.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    canvasPaneMouseClicked(evt);
                }
            });
            redraw();
        }

        return canvasPane;
    }

    @SuppressWarnings("unused")
    private JPopupMenu getCanvasPopupMenu() {
        if (canvasPopupMenu == null) {
            canvasPopupMenu = new JPopupMenu();
            canvasPopupMenu.add(getInsertBeforeMenuItem());
            canvasPopupMenu.add(getInsertAfterMenuItem());
            canvasPopupMenu.add(getInsertBranchMenuItem());
            canvasPopupMenu.add(getEditMenuItem());
            canvasPopupMenu.add(getDeleteMenuItem());
        }

        return canvasPopupMenu;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    @SuppressWarnings("unused")
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {

        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    Component selectedComponent = canvasPane.getComponentAt(e.getX(), e.getY());
                    if (selectedComponent instanceof PipelineNodeWidget) {
                        selectedNodeWidget = (PipelineNodeWidget) selectedComponent;
                        enableMenuItemsPerContext(selectedNodeWidget);
                        menu.show(parent, e.getX(), e.getY());
                    }
                }
            }
        });
    }

    private JMenuItem getEditMenuItem() {
        if (editMenuItem == null) {
            editMenuItem = new JMenuItem();
            editMenuItem.setText("View/Edit Node...");
            editMenuItem.addActionListener(this::edit);
        }

        return editMenuItem;
    }

    private JMenuItem getDeleteMenuItem() {
        if (deleteMenuItem == null) {
            deleteMenuItem = new JMenuItem();
            deleteMenuItem.setText("Delete Node...");
            deleteMenuItem.addActionListener(this::delete);
        }

        return deleteMenuItem;
    }

    private JMenuItem getInsertBranchMenuItem() {
        if (insertBranchMenuItem == null) {
            insertBranchMenuItem = new JMenuItem();
            insertBranchMenuItem.setText("Insert branch...");
            insertBranchMenuItem.addActionListener(this::insertBranch);
        }

        return insertBranchMenuItem;
    }

    private JMenuItem getInsertBeforeMenuItem() {
        if (insertBeforeMenuItem == null) {
            insertBeforeMenuItem = new JMenuItem();
            insertBeforeMenuItem.setText("Insert before...");
            insertBeforeMenuItem.addActionListener(this::insertBefore);
        }

        return insertBeforeMenuItem;
    }

    private JMenuItem getInsertAfterMenuItem() {
        if (insertAfterMenuItem == null) {
            insertAfterMenuItem = new JMenuItem();
            insertAfterMenuItem.setText("Insert after...");
            insertAfterMenuItem.addActionListener(this::insertAfter);
        }

        return insertAfterMenuItem;
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }
}
