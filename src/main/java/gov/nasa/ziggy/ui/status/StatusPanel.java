package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CONTAINER_GAP;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.Cursor;
import java.util.function.Supplier;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListCellRenderer;
import javax.swing.ListModel;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.status.Indicator.IdiotLight;
import gov.nasa.ziggy.ui.status.Indicator.IndicatorListener;
import gov.nasa.ziggy.ui.status.Indicator.State;
import gov.nasa.ziggy.ui.util.InstanceUpdateMessage;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * This class provides a color-coded display that provides real-time status of pipeline elements,
 * including active pipelines, pipeline processes, worker threads, metrics, and alerts.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class StatusPanel extends javax.swing.JPanel {

    private static final long serialVersionUID = 20240328L;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(StatusPanel.class);

    private static final String WARNING_MESSAGE = "One or more tasks failed but execution continues";
    private static final String ERROR_MESSAGE = "One or more tasks failed, execution halted";

    // To add new panels:
    // 1. Add the item to this enum in the order that it should appear. The text is used in
    // navigation. Provide a couple of method references that create the menu item and content panel
    // respectively.
    public enum ContentItem {
        PIPELINES("Pipelines", ContentMenu::createPipelinesIndicator, PipelinesStatusPanel::new),
        WORKERS("Workers", ContentMenu::createWorkersIndicator, WorkerStatusPanel::new),
        PROCESSES("Processes", ContentMenu::createProcessesIndicator,
            ProcessesStatusPanel::getInstance),
        ALERTS("Alerts", ContentMenu::createAlertsIndicator, AlertsStatusPanel::new);

        private String label;
        private Indicator menuItem;
        private Component panel;

        ContentItem(String label, Supplier<Indicator> createMenuItem,
            Supplier<Component> createPanel) {
            this.label = label;
            menuItem = createMenuItem.get();
            panel = createPanel.get();
        }

        @Override
        public String toString() {
            return label;
        }

        public Indicator menuItem() {
            return menuItem;
        }

        public Component panel() {
            return panel;
        }
    }

    private ContentMenu contentMenu;

    public StatusPanel() {
        buildComponent();
        ZiggyMessenger.subscribe(InstanceUpdateMessage.class, this::updateInstancesStatusLight);
    }

    private void buildComponent() {
        ContentPanel contentPanel = new ContentPanel();
        contentMenu = new ContentMenu(contentPanel);
        for (ContentItem contentItem : ContentItem.values()) {
            contentItem.menuItem.addIndicatorListener(contentMenu);
        }
        JScrollPane contentMenuScrollPane = new JScrollPane(contentMenu);

        // Add breathing room.
        contentMenuScrollPane.setViewportBorder(
            BorderFactory.createLineBorder(contentMenu.getBackground(), CONTAINER_GAP));

        setLayout(new BorderLayout());
        add(contentMenuScrollPane, BorderLayout.WEST);
        add(contentPanel, BorderLayout.CENTER);
    }

    /**
     * Sets the "idiot light" for pipelines based on a PipelineInstance state. A gray indicator
     * indicates that the selected instance was completed; green indicates initialized, processing,
     * or queued; yellow indicates errors running status; red indicates stopped or errors stalled
     * state.
     */
    private void updateInstancesStatusLight(InstanceUpdateMessage message) {

        Indicator instancesIndicator = StatusPanel.ContentItem.PIPELINES.menuItem();
        switch (message.getInstanceState()) {
            case COMPLETED:
                instancesIndicator.setState(
                    message.isInstancesRemaining() ? Indicator.State.NORMAL : Indicator.State.IDLE);
                break;
            case INITIALIZED:
            case PROCESSING:
                instancesIndicator.setState(Indicator.State.NORMAL);
                break;
            case ERRORS_RUNNING:
                instancesIndicator.setState(Indicator.State.WARNING, WARNING_MESSAGE);
                break;
            case ERRORS_STALLED:
                instancesIndicator.setState(Indicator.State.ERROR, ERROR_MESSAGE);
                break;
            default:
                throw new IllegalStateException(
                    "Unsupported pipeline instance state " + message.getInstanceState().toString());
        }
    }

    /**
     * The navigation menu for the status display.
     *
     * @author Bill Wohler
     */
    private static class ContentMenu extends JList<ContentItem>
        implements ListSelectionListener, ListCellRenderer<ContentItem>, IndicatorListener {

        private static final long serialVersionUID = 20240328L;

        @SuppressWarnings("unused")
        private static final Logger log = LoggerFactory.getLogger(ContentMenu.class);

        private ContentPanel contentPanel;

        public ContentMenu(ContentPanel contentPanel) {
            super(createModel());
            this.contentPanel = contentPanel;
            getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            setVisibleRowCount(-1);
            addListSelectionListener(this);
            setCellRenderer(this);
        }

        private static ListModel<ContentItem> createModel() {
            DefaultListModel<ContentItem> model = new DefaultListModel<>();
            for (ContentItem value : ContentItem.values()) {
                model.addElement(value);
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

        @Override
        public void stateChanged() {
            repaint();
        }

        private static Indicator createPipelinesIndicator() {
            return createIndicator("Pipelines", "Pipeline instance(s) executing normally",
                "No pipeline instances executing", Indicator.State.IDLE);
        }

        private static Indicator createWorkersIndicator() {
            return createIndicator("Workers", "Worker threads active", "All worker threads idle",
                Indicator.State.IDLE);
        }

        private static Indicator createProcessesIndicator() {
            return createIndicator("Processes", "All processes running normally", null,
                Indicator.State.NORMAL);
        }

        private static Indicator createAlertsIndicator() {
            return createIndicator("Alerts", "No unacknowledged alerts present", null,
                Indicator.State.NORMAL);
        }

        private static Indicator createIndicator(String name, String normalStateToolTipText,
            String idleStateToolTipText, State state) {
            Indicator indicator = new Indicator(name);
            indicator.singleLine();
            indicator.setIdiotLight(new IdiotLight());
            indicator.setNormalStateToolTipText(normalStateToolTipText);
            indicator.setIdleStateToolTipText(idleStateToolTipText);
            indicator.setState(state);
            return indicator;
        }

        @Override
        public Component getListCellRendererComponent(JList<? extends ContentItem> list,
            ContentItem value, int index, boolean isSelected, boolean cellHasFocus) {
            Component menuItem = value.menuItem();
            if (isSelected) {
                menuItem.setBackground(list.getSelectionBackground());
                menuItem.setForeground(list.getSelectionForeground());
            } else {
                menuItem.setBackground(list.getBackground());
                menuItem.setForeground(list.getForeground());
            }
            return menuItem;
        }
    }

    /**
     * The status content panel.
     *
     * @author Bill Wohler
     */
    private static class ContentPanel extends javax.swing.JPanel {
        private static final long serialVersionUID = 20240328L;
        private static final Logger log = LoggerFactory.getLogger(ContentPanel.class);

        public ContentPanel() {
            buildComponent();
        }

        private void buildComponent() {
            setLayout(new CardLayout());

            for (ContentItem value : ContentItem.values()) {
                add(value.panel(), value.toString());
            }
        }

        public void select(Object selection) {
            log.debug("selection={}", selection);
            ContentItem item = (ContentItem) selection;
            ((CardLayout) getLayout()).show(this, item.toString());
        }
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new StatusPanel());
    }
}
