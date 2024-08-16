package gov.nasa.ziggy.ui;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CONTAINER_GAP;

import java.awt.CardLayout;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.util.function.Supplier;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListModel;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.datastore.ViewEditDatastorePanel;
import gov.nasa.ziggy.ui.dr.DataReceiptPanel;
import gov.nasa.ziggy.ui.events.ZiggyEventHandlerPanel;
import gov.nasa.ziggy.ui.instances.InstancesTasksPanel;
import gov.nasa.ziggy.ui.metrilyzer.MetrilyzerPanel;
import gov.nasa.ziggy.ui.module.ViewEditModuleLibraryPanel;
import gov.nasa.ziggy.ui.parameters.ViewEditParameterSetsPanel;
import gov.nasa.ziggy.ui.pipeline.ViewEditPipelinesPanel;
import gov.nasa.ziggy.ui.status.StatusPanel;
import gov.nasa.ziggy.ui.util.ViewEditKeyValuePairPanel;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * The main content panel for the Ziggy console.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ZiggyConsolePanel extends JSplitPane {

    /** Initial location of divider between navigation menu and panels. */
    private static final int DIVIDER_LOCATION = 150;

    // To add new panels:
    // 1. Add the item to this enum in the order that it should appear. The text is used in
    // navigation. By default, an item is visible in the navigation and contains content. To hide
    // the item, set the visible flag to false. If the item doesn't have a panel associated with it,
    // then set the content to false to display the configured logo. If content is true, provide a
    // method reference to a method or constructor that returns a Component.
    private enum ContentItem {
        LOGO("Logo", false, true, ContentPanel::createLogoCard),
        PARAMETER_LIBRARY("Parameter Library", ViewEditParameterSetsPanel::newInstance),
        DATASTORE("Datastore", ViewEditDatastorePanel::new),
        PIPELINES("Pipelines", ViewEditPipelinesPanel::newInstance),
        INSTANCES("Instances", InstancesTasksPanel::new),
        STATUS("Status", StatusPanel::new),
        DATA_RECEIPT("Data Receipt", DataReceiptPanel::new),
        EVENT_HANDLERS("Event Definitions", ZiggyEventHandlerPanel::new),
        MODULE_LIBRARY("Module Library", ViewEditModuleLibraryPanel::new),
        METRILYZER("Metrilyzer", false, true, MetrilyzerPanel::new),
        GENERAL("General", false, true, ViewEditKeyValuePairPanel::new);

        private String label;
        private boolean visible;
        private boolean content;
        private Supplier<Component> create;

        ContentItem(String label, Supplier<Component> create) {
            this(label, true, true, create);
        }

        ContentItem(String label, boolean visible, boolean content, Supplier<Component> create) {
            this.label = label;
            this.visible = visible;
            this.content = content;
            this.create = create;
        }

        @Override
        public String toString() {
            return label;
        }

        public boolean isVisible() {
            return visible;
        }

        boolean hasContent() {
            return content;
        }

        public Supplier<Component> create() {
            return create;
        }
    }

    private ContentMenu contentMenu;

    public ZiggyConsolePanel() {
        ContentPanel contentPanel = new ContentPanel();
        contentMenu = new ContentMenu(contentPanel);
        JScrollPane contentMenuScrollPane = new JScrollPane(contentMenu);

        // Add breathing room.
        contentMenuScrollPane.setViewportBorder(
            BorderFactory.createLineBorder(contentMenu.getBackground(), CONTAINER_GAP));

        add(contentMenuScrollPane, JSplitPane.LEFT);
        add(contentPanel, JSplitPane.RIGHT);

        setDividerLocation(DIVIDER_LOCATION);
        setOneTouchExpandable(true);
    }

    public ContentMenu getContentMenu() {
        return contentMenu;
    }

    public static void main(String[] args) {
        try {
            ZiggySwingUtils.displayTestDialog(new ContentPanel());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * The navigation menu for the console.
     *
     * @author Todd Klaus
     * @author Bill Wohler
     */
    private static class ContentMenu extends JList<ContentItem> implements ListSelectionListener {

        @SuppressWarnings("unused")
        private static final Logger log = LoggerFactory.getLogger(ContentMenu.class);

        private ContentPanel contentPanel;

        public ContentMenu(ContentPanel contentPanel) {
            super(createModel());
            this.contentPanel = contentPanel;
            getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            setVisibleRowCount(-1);
            addListSelectionListener(this);
        }

        private static ListModel<ContentItem> createModel() {
            DefaultListModel<ContentItem> model = new DefaultListModel<>();
            for (ContentItem value : ContentItem.values()) {
                if (value.isVisible()) {
                    model.addElement(value);
                }
            }
            return model;
        }

        @Override
        public void valueChanged(ListSelectionEvent evt) {
            if (evt.getValueIsAdjusting() || getSelectedValue() == null) {
                return;
            }

            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            contentPanel.select(getSelectedValue());
            setCursor(null);
        }
    }

    /**
     * The main Ziggy GUI content panel.
     *
     * @author Todd Klaus
     * @author Bill Wohler
     */
    private static class ContentPanel extends javax.swing.JPanel {
        private static final Logger log = LoggerFactory.getLogger(ContentPanel.class);

        private static int PREFERRED_WIDTH = 400;
        private static int PREFERRED_HEIGHT = 300;

        public ContentPanel() {
            buildComponent();
        }

        private void buildComponent() {
            setLayout(new CardLayout());
            setPreferredSize(new Dimension(PREFERRED_WIDTH, PREFERRED_HEIGHT));

            for (ContentItem value : ContentItem.values()) {
                add(value.create().get(), value.toString());
            }
        }

        private static JPanel createLogoCard() {
            ScalingImagePanel logoCard = new ScalingImagePanel(
                ZiggyGuiConsole.pipelineOrZiggyImage());
            logoCard.setLayout(new GridBagLayout());
            logoCard.setBackground(new java.awt.Color(255, 255, 255));

            return logoCard;
        }

        public void select(Object selection) {
            log.debug("selection={}", selection);
            ContentItem item = (ContentItem) selection;
            ((CardLayout) getLayout()).show(this,
                item.hasContent() ? item.toString() : ContentItem.LOGO.toString());
        }

        private static class ScalingImagePanel extends JPanel {

            private final Image image;

            public ScalingImagePanel(Image image) {
                this.image = image;
            }

            @Override
            public void paintComponent(Graphics g) {
                super.paintComponent(g);

                // Determine scale to preserve aspect ratio of image.
                double scale = Math.min((double) getWidth() / image.getWidth(null),
                    (double) getHeight() / image.getHeight(null));

                // Fit and center image to panel while honoring aspect ratio.
                int newWidth = (int) (scale * image.getWidth(null));
                int newHeight = (int) (scale * image.getHeight(null));
                g.drawImage(image.getScaledInstance(newWidth, newHeight, Image.SCALE_SMOOTH),
                    (getWidth() - newWidth) / 2, (getHeight() - newHeight) / 2, null);
            }
        }
    }
}
