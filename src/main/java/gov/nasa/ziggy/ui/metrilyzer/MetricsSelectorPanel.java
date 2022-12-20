package gov.nasa.ziggy.ui.metrilyzer;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.ui.common.DateTextField;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Allow user to select metrics. Update display when metrics are plotted. And allow user to compute
 * the time interval for selected metrics.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
public class MetricsSelectorPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(MetricsSelectorPanel.class);

    private static final long DEFAULT_WINDOW_SIZE_MILLIS = 2 /* hrs */ * 60 /* mins/hr */
        * 60 /* secs/min */ * 1000 /* ms/sec */;

    private JPanel listPanel;
    private JPanel filterPanel;
    private DateTextField endDateTextField;
    private JLabel endLabel;
    private DateTextField startDateTextField;
    private JLabel startLabel;
    private JButton removeButton;
    private JButton addButton;
    private JList<String> selectedList;
    private JButton plotButton;
    private JButton maxButton;
    private JButton loadButton;
    private JPanel loadFilterPanel;
    private JButton windowRightButton;
    private JButton windowLeftButton;
    private JList<String> availList;
    private JScrollPane selectedScrollPane;
    private JScrollPane availScrollPane;
    private MetricsChartPanel chart = null;
    private final MetricTypeListModel availListModel;
    private final MetricTypeListModel selectedListModel;
    private long windowStart = 0;
    private JButton lastTwoHoursButton;
    private JButton lastHourButton;
    private JLabel binSizeLabel;
    private JTextField binSizeTextField;
    private JCheckBox binCheckBox;
    private JPanel actionPanel;
    private JPanel windowPanel;
    private JPanel binPanel;
    private long windowEnd = 0;
    private long currentWindowSize = DEFAULT_WINDOW_SIZE_MILLIS;
    private final MetricsValueSource metricsValueSource;

    /**
     * @param chart
     */
    public MetricsSelectorPanel(MetricTypeListModel availListModel,
        MetricTypeListModel selectedListModel, MetricsValueSource metricsValueSource,
        MetricsChartPanel chart) {
        this.chart = chart;
        this.availListModel = availListModel;
        this.selectedListModel = selectedListModel;
        this.metricsValueSource = metricsValueSource;
        if (availListModel == null) {
            throw new NullPointerException("availListModel");
        }
        if (selectedListModel == null) {
            throw new NullPointerException("selectedListModel");
        }
        if (metricsValueSource == null) {
            throw new NullPointerException("metricsValueSource");
        }
        initGUI();
    }

    public MetricsSelectorPanel(MetricTypeListModel availListModel,
        MetricTypeListModel selectedListModel, MetricsValueSource metricsValueSource) {
        this(availListModel, selectedListModel, metricsValueSource, null);
    }

    private void initGUI() {
        try {
            windowEnd = System.currentTimeMillis();
            windowStart = windowEnd - currentWindowSize;

            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(600, 300));
            this.add(getListPanel(), BorderLayout.CENTER);
            this.add(getFilterPanel(), BorderLayout.SOUTH);
            this.add(getLoadFilterPanel(), BorderLayout.NORTH);
        } catch (Exception e) {
            log.error("initGUI()", e);
        }
    }

    private JPanel getListPanel() {
        if (listPanel == null) {
            listPanel = new JPanel();
            GridBagLayout listPanelLayout = new GridBagLayout();
            listPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            listPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7 };
            listPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1 };
            listPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            listPanel.setLayout(listPanelLayout);
            listPanel.add(getAvailScrollPane(), new GridBagConstraints(0, 0, 4, 6, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            listPanel.add(getSelectedScrollPane(), new GridBagConstraints(5, 0, 4, 6, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            listPanel.add(getAddButton(), new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            listPanel.add(getRemoveButton(), new GridBagConstraints(4, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return listPanel;
    }

    private JPanel getFilterPanel() {
        if (filterPanel == null) {
            filterPanel = new JPanel();
            filterPanel.add(getBinPanel());
            filterPanel.add(getWindowPanel());
            filterPanel.add(getActionPanel());
        }
        return filterPanel;
    }

    private JScrollPane getAvailScrollPane() {
        if (availScrollPane == null) {
            availScrollPane = new JScrollPane();
            availScrollPane.setBorder(BorderFactory.createTitledBorder("Available Metrics"));
            availScrollPane.setViewportView(getAvailList());
        }
        return availScrollPane;
    }

    private JScrollPane getSelectedScrollPane() {
        if (selectedScrollPane == null) {
            selectedScrollPane = new JScrollPane();
            selectedScrollPane.setBorder(BorderFactory.createTitledBorder("Selected Metrics"));
            selectedScrollPane.setViewportView(getSelectedList());
        }
        return selectedScrollPane;
    }

    private JList<String> getAvailList() {
        if (availList == null) {
            availList = new JList<>();
            availList.setModel(availListModel);
        }
        return availList;
    }

    private JList<String> getSelectedList() {
        if (selectedList == null) {
            selectedList = new JList<>();
            selectedList.setModel(selectedListModel);
        }
        return selectedList;
    }

    private JButton getAddButton() {
        if (addButton == null) {
            addButton = new JButton();
            addButton.setText(">");
            addButton.addActionListener(this::addButtonActionPerformed);
        }
        return addButton;
    }

    private JButton getRemoveButton() {
        if (removeButton == null) {
            removeButton = new JButton();
            removeButton.setText("<");
            removeButton.addActionListener(this::removeButtonActionPerformed);
        }
        return removeButton;
    }

    private JLabel getStartLabel() {
        if (startLabel == null) {
            startLabel = new JLabel();
            startLabel.setText("start");
        }
        return startLabel;
    }

    private DateTextField getStartDateTextField() {
        if (startDateTextField == null) {
            startDateTextField = new DateTextField(new Date(windowStart));
        }
        return startDateTextField;
    }

    private JLabel getEndLabel() {
        if (endLabel == null) {
            endLabel = new JLabel();
            endLabel.setText("end");
        }
        return endLabel;
    }

    private DateTextField getEndDateTextField() {
        if (endDateTextField == null) {
            endDateTextField = new DateTextField(new Date(windowEnd));
        }
        return endDateTextField;
    }

    private JButton getWindowLeftButton() {
        if (windowLeftButton == null) {
            windowLeftButton = new JButton();
            windowLeftButton.setText("<");
            windowLeftButton.addActionListener(this::windowLeftButtonActionPerformed);
        }
        return windowLeftButton;
    }

    private JButton getWindowRightButton() {
        if (windowRightButton == null) {
            windowRightButton = new JButton();
            windowRightButton.setText(">");
            windowRightButton.addActionListener(this::windowRightButtonActionPerformed);
        }
        return windowRightButton;
    }

    private JButton getPlotButton() {
        if (plotButton == null) {
            plotButton = new JButton();
            plotButton.setText("Plot");
            plotButton.setFont(new java.awt.Font("Dialog", 1, 14));
            plotButton.addActionListener(this::plotButtonActionPerformed);
        }
        return plotButton;
    }

    private JPanel getLoadFilterPanel() {
        if (loadFilterPanel == null) {
            loadFilterPanel = new JPanel();
            FlowLayout loadFilterPanelLayout = new FlowLayout();
            loadFilterPanelLayout.setAlignment(FlowLayout.LEFT);
            loadFilterPanel.setLayout(loadFilterPanelLayout);
            loadFilterPanel.add(getLoadButton());
        }
        return loadFilterPanel;
    }

    private JButton getLoadButton() {
        if (loadButton == null) {
            loadButton = new JButton();
            loadButton.setText("Load");
            loadButton.addActionListener(this::loadButtonActionPerformed);
        }
        return loadButton;
    }

    private JButton getMaxButton() {
        if (maxButton == null) {
            maxButton = new JButton();
            maxButton.setText("max");
            maxButton.addActionListener(evt -> maxButtonActionPerformed());
        }
        return maxButton;
    }

    private JPanel getBinPanel() {
        if (binPanel == null) {
            binPanel = new JPanel();
            FlowLayout binPanelLayout = new FlowLayout();
            binPanel.setLayout(binPanelLayout);
            binPanel.setBorder(BorderFactory.createTitledBorder("bin"));
            binPanel.add(getBinCheckBox());
            binPanel.add(getBinSizeTextField());
            binPanel.add(getBinSizeLabel());
        }
        return binPanel;
    }

    private JPanel getWindowPanel() {
        if (windowPanel == null) {
            windowPanel = new JPanel();
            windowPanel.setBorder(BorderFactory.createTitledBorder("time window"));
            windowPanel.add(getStartLabel());
            windowPanel.add(getStartDateTextField());
            windowPanel.add(getEndLabel());
            windowPanel.add(getEndDateTextField());
            windowPanel.add(getMaxButton());
            windowPanel.add(getWindowLeftButton());
            windowPanel.add(getWindowRightButton());
            windowPanel.add(getLastHourButton());
            windowPanel.add(getLastTwoHoursButton());
        }
        return windowPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            actionPanel.add(getPlotButton());
        }
        return actionPanel;
    }

    private JCheckBox getBinCheckBox() {
        if (binCheckBox == null) {
            binCheckBox = new JCheckBox();
            binCheckBox.setSelected(false);
        }
        return binCheckBox;
    }

    private JTextField getBinSizeTextField() {
        if (binSizeTextField == null) {
            binSizeTextField = new JTextField();
            binSizeTextField.setColumns(4);
            binSizeTextField.setText("60");
        }
        return binSizeTextField;
    }

    private JLabel getBinSizeLabel() {
        if (binSizeLabel == null) {
            binSizeLabel = new JLabel();
            binSizeLabel.setText("secs");
        }
        return binSizeLabel;
    }

    private JButton getLastHourButton() {
        if (lastHourButton == null) {
            lastHourButton = new JButton();
            lastHourButton.setText("1hr");
            lastHourButton.addActionListener(evt -> lastHourButtonActionPerformed());
        }
        return lastHourButton;
    }

    private JButton getLastTwoHoursButton() {
        if (lastTwoHoursButton == null) {
            lastTwoHoursButton = new JButton();
            lastTwoHoursButton.setText("2hr");
            lastTwoHoursButton.addActionListener(evt -> lastTwoHoursButtonActionPerformed());
        }
        return lastTwoHoursButton;
    }

    private void loadButtonActionPerformed(ActionEvent evt) {
        log.debug(
            "loadButtonActionPerformed(ActionEvent) - loadButton.actionPerformed, event=" + evt);

        try {
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            availListModel.loadMetricTypes();
        } catch (PipelineException e) {
            MessageUtil.showError(this, e);
        } finally {
            setCursor(null);
        }
    }

    private void addButtonActionPerformed(ActionEvent evt) {
        log.debug(
            "addButtonActionPerformed(ActionEvent) - addButton.actionPerformed, event=" + evt);

        if (availListModel.getSize() <= 0) {
            return;
        }

        int availIndex = availList.getSelectedIndex();

        if (availIndex == -1) {
            return;
        }

        selectedListModel.add(availListModel.remove(availIndex));
    }

    private void removeButtonActionPerformed(ActionEvent evt) {
        log.debug("removeButtonActionPerformed(ActionEvent) - removeButton.actionPerformed, event="
            + evt);

        if (selectedListModel.getSize() <= 0) {
            return;
        }

        int selectedIndex = selectedList.getSelectedIndex();

        if (selectedIndex == -1) {
            return;
        }

        availListModel.add(selectedListModel.remove(selectedIndex));
    }

    private void windowLeftButtonActionPerformed(ActionEvent evt) {
        log.debug(
            "windowLeftButtonActionPerformed(ActionEvent) - windowLeftButton.actionPerformed, event="
                + evt);

        try {
            updateWindow();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        windowStart -= currentWindowSize;
        startDateTextField.setDate(new Date(windowStart));
        windowEnd -= currentWindowSize;
        endDateTextField.setDate(new Date(windowEnd));
    }

    private void windowRightButtonActionPerformed(ActionEvent evt) {
        log.debug(
            "windowRightButtonActionPerformed(ActionEvent) - windowRightButton.actionPerformed, event="
                + evt);

        try {
            updateWindow();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        windowStart += currentWindowSize;
        startDateTextField.setDate(new Date(windowStart));
        windowEnd += currentWindowSize;
        endDateTextField.setDate(new Date(windowEnd));
    }

    private void plotButtonActionPerformed(ActionEvent evt) {
        log.debug(
            "refreshButtonActionPerformed(ActionEvent) - plotButton.actionPerformed, event=" + evt);

        try {
            boolean binEnabled = binCheckBox.isSelected();
            int binSizeMillis = 0;

            if (binEnabled) {
                binSizeMillis = Integer.parseInt(binSizeTextField.getText()) * 1000;
            }

            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

            updateWindow();

            // do queries & update chart
            chart.clearChart();
            List<MetricType> types = selectedListModel.getTypes();
            Map<MetricType, Collection<MetricValue>> selectedMetricValues = metricsValueSource
                .metricValues(types, new Date(windowStart), new Date(windowEnd));
            for (Map.Entry<MetricType, Collection<MetricValue>> metricValues : selectedMetricValues
                .entrySet()) {
                chart.addMetric(metricValues.getKey().getName(), metricValues.getValue(),
                    binSizeMillis);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        } finally {
            setCursor(null);
        }
    }

    private void updateWindow() throws ParseException {
        Date startDate = startDateTextField.getDate();
        Date endDate = endDateTextField.getDate();

        windowStart = startDate.getTime();
        windowEnd = endDate.getTime();
        currentWindowSize = windowEnd - windowStart;
    }

    private void maxButtonActionPerformed() {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        try {
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

            List<MetricType> types = selectedListModel.getTypes();
            Map<MetricType, TimeRange> metricTypeToDateRange = metricsValueSource
                .metricStartEndDates(types);
            for (MetricType metricType : types) {
                TimeRange range = metricTypeToDateRange.get(metricType);
                Date startDate = range.getStartTimestamp();
                if (startDate != null && startDate.getTime() < min) {
                    min = startDate.getTime();
                }
                Date endDate = range.getEndTimestamp();
                if (endDate != null && endDate.getTime() > max) {
                    max = endDate.getTime();
                }
            }

            windowStart = min;
            windowEnd = max;
            startDateTextField.setDate(new Date(windowStart));
            endDateTextField.setDate(new Date(windowEnd));
            currentWindowSize = windowEnd - windowStart;
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        } finally {
            setCursor(null);
        }
    }

    private void lastHourButtonActionPerformed() {
        long now = System.currentTimeMillis();

        windowStart = now - 60 * 60 * 1000;
        windowEnd = now;
        startDateTextField.setDate(new Date(windowStart));
        endDateTextField.setDate(new Date(windowEnd));
        currentWindowSize = windowEnd - windowStart;
    }

    private void lastTwoHoursButtonActionPerformed() {
        long now = System.currentTimeMillis();

        windowStart = now - 2 * 60 * 60 * 1000;
        windowEnd = now;
        startDateTextField.setDate(new Date(windowStart));
        endDateTextField.setDate(new Date(windowEnd));
        currentWindowSize = windowEnd - windowStart;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        DatabaseMetricsTypeListModel availMetrics = new DatabaseMetricsTypeListModel();
        DatabaseMetricsTypeListModel selectedMetrics = new DatabaseMetricsTypeListModel();
        DatabaseMetricsValueSource metricsSource = new DatabaseMetricsValueSource();
        frame.getContentPane()
            .add(new MetricsSelectorPanel(availMetrics, selectedMetrics, metricsSource));
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
