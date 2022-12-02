package gov.nasa.ziggy.ui.config.pipeline;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * Display the pipeline nodes as a directed graph with pop-up menus on each node.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineGraphCanvas extends JPanel {
    private static final Logger log = LoggerFactory.getLogger(PipelineGraphCanvas.class);

    // size of the canvas (TODO: make dynamic based on # nodes)
    private static final int CANVAS_HEIGHT = 5000;
    private static final int CANVAS_WIDTH = 1500;

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

    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    private enum DrawType {
        LINES, NODES
    }

    public PipelineGraphCanvas(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        BorderLayout thisLayout = new BorderLayout();
        setLayout(thisLayout);
        scrollPane = getScrollPane();
        add(scrollPane, BorderLayout.CENTER);

        setComponentPopupMenu(canvasPane, getCanvasPopupMenu());

        log.debug("initGUI() - end");
    }

    public void redraw() {
        log.debug("redraw() - start");

        canvasPane.removeAll();
        drawPipeline(pipeline, DrawType.NODES, null);
        revalidate();
        repaint();

        log.debug("redraw() - end");
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
        log.debug("drawNodes(PipelineNode, int, int, DrawType, Rectangle, Graphics2D) - start");

        Rectangle nodeBounds = new Rectangle(HORIZONTAL_INSET + COLUMN_SPACING * column,
            VERTICAL_INSET + ROW_SPACING * row, NODE_WIDGET_WIDTH, NODE_WIDGET_HEIGHT);

        if (drawType == DrawType.NODES) {
            PipelineNodeWidget widget = new PipelineNodeWidget(node, parentNode);
            canvasPane.add(widget);
            widget.setBounds(nodeBounds);
        } else { // LINES
            if (parentBounds != null) {
                drawArrow(node.isStartNewUow(), g2, parentBounds, nodeBounds);
            }
        }

        int childColumn = column;

        for (PipelineDefinitionNode childNode : node.getNextNodes()) {
            drawNodes(childNode, node, row + 1, childColumn, drawType, nodeBounds, g2);
            childColumn++;
        }

        log.debug("drawNodes(PipelineNode, int, int, DrawType, Rectangle, Graphics2D) - end");
    }

    private void drawArrow(boolean startNewUow, Graphics2D g2, Rectangle start, Rectangle end) {
        log.debug("drawArrow(Graphics2D, Rectangle, Rectangle) - start");

        Stroke oldStroke = g2.getStroke();

        BasicStroke lineStroke;

        if (startNewUow) {
            lineStroke = new BasicStroke(2);
        } else {
            lineStroke = new BasicStroke(2, BasicStroke.CAP_SQUARE, BasicStroke.JOIN_MITER, 10.0f,
                new float[] { 10 }, 0.0f);
        }

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

        log.debug("drawArrow(Graphics2D, Rectangle, Rectangle) - end");
    }

    protected void doEdit() {
        log.debug("doEdit(int) - start");

        showEditDialog(selectedNodeWidget.getPipelineNode());

        log.debug("doEdit(int) - end");
    }

    protected void doDelete() {
        log.debug("doDelete(int) - start");

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

                pipelineDefinitionCrud.deletePipelineNode(selectedNode);

                pipeline.buildPaths();
                redraw();
            }
        } catch (Throwable e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("doDelete(int) - end");
    }

    private void doInsertBefore() {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode predecessorNode = selectedNodeWidget.getPipelineNodeParent();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();
            newNode.setParentNode(predecessorNode);
            newNode.setStartNewUow(true);

            PipelineNodeEditDialog pipelineNodeEditDialog = ZiggyGuiConsole
                .newPipelineNodeEditDialog(pipeline, newNode);
            pipelineNodeEditDialog.setVisible(true);

            if (pipelineNodeEditDialog.wasSavePressed()) {
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
                assert selectedNodeIndex != -1 : "PipelineNodesViewEditPanel:doInsertBefore: predecessor node does not point to this node!";

                predecessorsNextNodesList.set(selectedNodeIndex, newNode);

                // set new node's nextNodes to this node
                newNode.getNextNodes().add(selectedNode);

                pipelineDefinitionCrud.save(pipeline);
                pipeline.buildPaths();
                redraw();
            }
        } catch (Throwable e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }
    }

    private void doInsertAfter() {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();
            newNode.setParentNode(selectedNode);
            newNode.setStartNewUow(true);

            PipelineNodeEditDialog pipelineNodeEditDialog = ZiggyGuiConsole
                .newPipelineNodeEditDialog(pipeline, newNode);
            pipelineNodeEditDialog.setVisible(true);

            if (pipelineNodeEditDialog.wasSavePressed()) {
                List<PipelineDefinitionNode> currentNextNodes = null;

                if (selectedNodeWidget.isStartNode()) {
                    currentNextNodes = selectedNodeWidget.getPipeline().getRootNodes();
                } else {
                    currentNextNodes = selectedNode.getNextNodes();
                }

                // copy selected node's nextNodes to new node's nextNodes
                List<PipelineDefinitionNode> newNodeNextNodes = new ArrayList<>();
                newNodeNextNodes.addAll(currentNextNodes);
                newNode.setNextNodes(newNodeNextNodes);

                // set selected node's nextNodes to new node
                List<PipelineDefinitionNode> selectedNodeNextNodes = currentNextNodes;
                selectedNodeNextNodes.clear();
                selectedNodeNextNodes.add(newNode);

                pipelineDefinitionCrud.save(pipeline);
                pipeline.buildPaths();
                redraw();
            }
        } catch (Throwable e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }
    }

    private void doInsertBranch() {
        try {
            PipelineDefinitionNode selectedNode = selectedNodeWidget.getPipelineNode();
            PipelineDefinitionNode newNode = new PipelineDefinitionNode();
            newNode.setParentNode(selectedNode);
            newNode.setStartNewUow(true);

            PipelineNodeEditDialog pipelineNodeEditDialog = ZiggyGuiConsole
                .newPipelineNodeEditDialog(pipeline, newNode);
            pipelineNodeEditDialog.setVisible(true);

            if (pipelineNodeEditDialog.wasSavePressed()) {
                List<PipelineDefinitionNode> currentNextNodes = null;

                if (selectedNodeWidget.isStartNode()) {
                    currentNextNodes = selectedNodeWidget.getPipeline().getRootNodes();
                } else {
                    currentNextNodes = selectedNode.getNextNodes();
                }

                // add new node to selected node's nextNodes
                currentNextNodes.add(newNode);

                pipelineDefinitionCrud.save(pipeline);
                pipeline.buildPaths();
                redraw();
            }
        } catch (Throwable e) {
            log.error("doInsertBranch(User)", e);

            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(PipelineDefinitionNode pipelineNode) {
        log.debug("showEditDialog() - start");

        try {
            PipelineNodeEditDialog inst = ZiggyGuiConsole.newPipelineNodeEditDialog(pipeline,
                pipelineNode);

            inst.setVisible(true);

            if (inst.wasSavePressed()) {
                pipelineDefinitionCrud.save(pipeline);
                redraw();
            }
        } catch (Throwable e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
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

    private void editMenuItemActionPerformed(ActionEvent evt) {
        log.debug("editMenuItemActionPerformed(ActionEvent) - editMenuItem.actionPerformed, event="
            + evt);

        doEdit();
    }

    private void deleteMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "deleteMenuItemActionPerformed(ActionEvent) - deleteMenuItem.actionPerformed, event="
                + evt);

        doDelete();
    }

    private void insertBeforeMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "insertBeforeMenuItemActionPerformed(ActionEvent) - insertBeforeMenuItem.actionPerformed, event="
                + evt);
        doInsertBefore();
    }

    private void insertAfterMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "insertAfterMenuItemActionPerformed(ActionEvent) - insertAfterMenuItem.actionPerformed, event="
                + evt);
        doInsertAfter();
    }

    private void insertBranchMenuItemActionPerformed(ActionEvent evt) {
        log.debug(
            "insertBranchMenuItemActionPerformed(ActionEvent) - insertBranchMenuItem.actionPerformed, event="
                + evt);
        doInsertBranch();
    }

    private void canvasPaneMouseClicked(MouseEvent evt) {
        log.debug("canvasPaneMouseClicked(MouseEvent) - canvasPane.mouseClicked, event=" + evt);

        if (evt.getClickCount() == 2) {
            log.debug(
                "canvasPaneMouseClicked(MouseEvent) - [DOUBLE-CLICK] table.mouseClicked, event="
                    + evt);

            Component selectedComponent = canvasPane.getComponentAt(evt.getX(), evt.getY());
            log.debug("MP:selectedComponent = " + selectedComponent);
            if (selectedComponent instanceof PipelineNodeWidget) {
                selectedNodeWidget = (PipelineNodeWidget) selectedComponent;

                doEdit();
            }
        }
    }

    private JScrollPane getScrollPane() {
        log.debug("getScrollPane() - start");

        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getCanvasPane());
        }

        log.debug("getScrollPane() - end");
        return scrollPane;
    }

    private JPanel getCanvasPane() {
        log.debug("getCanvasPane() - start");

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

        log.debug("getCanvasPane() - end");
        return canvasPane;
    }

    private JPopupMenu getCanvasPopupMenu() {
        log.debug("getCanvasPopupMenu() - start");

        if (canvasPopupMenu == null) {
            canvasPopupMenu = new JPopupMenu();
            canvasPopupMenu.add(getInsertBeforeMenuItem());
            canvasPopupMenu.add(getInsertAfterMenuItem());
            canvasPopupMenu.add(getInsertBranchMenuItem());
            canvasPopupMenu.add(getEditMenuItem());
            canvasPopupMenu.add(getDeleteMenuItem());
        }

        log.debug("getCanvasPopupMenu() - end");
        return canvasPopupMenu;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        log.debug("setComponentPopupMenu(java.awt.Component, javax.swing.JPopupMenu) - start");

        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                log.debug("mousePressed(java.awt.event.MouseEvent) - start");

                if (e.isPopupTrigger()) {
                    Component selectedComponent = canvasPane.getComponentAt(e.getX(), e.getY());
                    log.debug("MP:selectedComponent = " + selectedComponent);
                    if (selectedComponent instanceof PipelineNodeWidget) {
                        selectedNodeWidget = (PipelineNodeWidget) selectedComponent;
                        enableMenuItemsPerContext(selectedNodeWidget);
                        menu.show(parent, e.getX(), e.getY());
                    }
                }

                log.debug("mousePressed(java.awt.event.MouseEvent) - end");
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                log.debug("mouseReleased(java.awt.event.MouseEvent) - start");

                if (e.isPopupTrigger()) {
                    Component selectedComponent = canvasPane.getComponentAt(e.getX(), e.getY());
                    log.debug("MR:selectedComponent = " + selectedComponent);
                    if (selectedComponent instanceof PipelineNodeWidget) {
                        selectedNodeWidget = (PipelineNodeWidget) selectedComponent;
                        enableMenuItemsPerContext(selectedNodeWidget);
                        menu.show(parent, e.getX(), e.getY());
                    }
                }

                log.debug("mouseReleased(java.awt.event.MouseEvent) - end");
            }
        });

        log.debug("setComponentPopupMenu(java.awt.Component, javax.swing.JPopupMenu) - end");
    }

    private JMenuItem getEditMenuItem() {
        log.debug("getEditMenuItem() - start");

        if (editMenuItem == null) {
            editMenuItem = new JMenuItem();
            editMenuItem.setText("View/Edit Node...");
            editMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                editMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getEditMenuItem() - end");
        return editMenuItem;
    }

    private JMenuItem getDeleteMenuItem() {
        log.debug("getDeleteMenuItem() - start");

        if (deleteMenuItem == null) {
            deleteMenuItem = new JMenuItem();
            deleteMenuItem.setText("Delete Node...");
            deleteMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                deleteMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getDeleteMenuItem() - end");
        return deleteMenuItem;
    }

    private JMenuItem getInsertBranchMenuItem() {
        log.debug("getInsertBranchMenuItem() - start");

        if (insertBranchMenuItem == null) {
            insertBranchMenuItem = new JMenuItem();
            insertBranchMenuItem.setText("Insert branch...");
            insertBranchMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                insertBranchMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getInsertBranchMenuItem() - end");
        return insertBranchMenuItem;
    }

    private JMenuItem getInsertBeforeMenuItem() {
        log.debug("getInsertBeforeMenuItem() - start");

        if (insertBeforeMenuItem == null) {
            insertBeforeMenuItem = new JMenuItem();
            insertBeforeMenuItem.setText("Insert before...");
            insertBeforeMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                insertBeforeMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getInsertBeforeMenuItem() - end");
        return insertBeforeMenuItem;
    }

    private JMenuItem getInsertAfterMenuItem() {
        log.debug("getInsertAfterMenuItem() - start");

        if (insertAfterMenuItem == null) {
            insertAfterMenuItem = new JMenuItem();
            insertAfterMenuItem.setText("Insert after...");
            insertAfterMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                insertAfterMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getInsertAfterMenuItem() - end");
        return insertAfterMenuItem;
    }
}
