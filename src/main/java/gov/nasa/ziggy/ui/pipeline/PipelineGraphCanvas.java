package gov.nasa.ziggy.ui.pipeline;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.Toolkit;

import javax.swing.JPanel;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

/**
 * Display the pipeline nodes as a directed graph.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class PipelineGraphCanvas extends JPanel {
    private enum DrawType {
        LINES, NODES
    }

    // Spacing at the top and left of the canvas.
    private final static int HORIZONTAL_INSET = 20;
    private final static int VERTICAL_INSET = 20;

    // Spacing between rows & columns of widgets.
    private final static int ROW_SPACING = 100;
    private final static int COLUMN_SPACING = 220;

    // Widget sizes.
    private final static int START_WIDGET_WIDTH = 100;
    private final static int START_WIDGET_HEIGHT = 30;
    private final static int NODE_WIDGET_WIDTH = 250;
    private final static int NODE_WIDGET_HEIGHT = 30;

    private final PipelineDefinition pipeline;
    private JPanel canvasPane;
    private int width;
    private int height = 2 * VERTICAL_INSET;

    public PipelineGraphCanvas(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
        buildComponent();
    }

    private void buildComponent() {
        setLayout(new BorderLayout());
        createCanvasPane();
        add(new JScrollPane(canvasPane), BorderLayout.CENTER);
    }

    private void createCanvasPane() {
        canvasPane = new JPanel() {
            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                // TODO Currently only supports one root node
                drawPipeline(pipeline, DrawType.LINES, (Graphics2D) g);
            }
        };
        canvasPane.setBackground(new java.awt.Color(255, 255, 255));
        canvasPane.setLayout(null);
        redraw();
    }

    private void redraw() {
        canvasPane.removeAll();
        drawPipeline(pipeline, DrawType.NODES, null);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        canvasPane.setPreferredSize(
            new Dimension(Math.min(width, screenSize.width), Math.min(height, screenSize.height)));
        revalidate();
        repaint();
    }

    private void drawPipeline(PipelineDefinition pipeline, DrawType drawType, Graphics2D g2) {
        int horzInset = HORIZONTAL_INSET + (NODE_WIDGET_WIDTH - START_WIDGET_WIDTH) / 2;
        Rectangle nodeBounds = new Rectangle(horzInset, VERTICAL_INSET, START_WIDGET_WIDTH,
            START_WIDGET_HEIGHT);

        if (drawType == DrawType.NODES) {
            PipelineNodeWidget widget = new PipelineNodeWidget();
            canvasPane.add(widget);
            adjustBounds(nodeBounds, widget, START_WIDGET_HEIGHT);
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
            PipelineNodeWidget widget = new PipelineNodeWidget(node);
            canvasPane.add(widget);
            adjustBounds(nodeBounds, widget, ROW_SPACING);
        } else if (parentBounds != null) {
            drawArrow(g2, parentBounds, nodeBounds);
        }

        int childColumn = column;

        for (PipelineDefinitionNode childNode : node.getNextNodes()) {
            drawNodes(childNode, node, row + 1, childColumn, drawType, nodeBounds, g2);
            childColumn++;
        }
    }

    private void adjustBounds(Rectangle nodeBounds, PipelineNodeWidget widget, int widgetHeight) {
        int widgetWidth = widget.getPreferredSize().width + 2 * HORIZONTAL_INSET;
        nodeBounds.width = widgetWidth;
        widget.setBounds(nodeBounds);
        // TODO Increase width if there are multiple children
        width = Math.max(width, widgetWidth + 2 * HORIZONTAL_INSET);
        height += widgetHeight;
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
}
