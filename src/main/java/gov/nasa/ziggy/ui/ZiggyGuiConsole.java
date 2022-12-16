package gov.nasa.ziggy.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.SwingConstants;
import javax.swing.plaf.metal.MetalLookAndFeel;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.looks.plastic.theme.SkyBluer;

import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;
import gov.nasa.ziggy.ui.common.DoubleListDialog;
import gov.nasa.ziggy.ui.common.GroupsDialog;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.ConfigDataPanel;
import gov.nasa.ziggy.ui.config.ConfigTree;
import gov.nasa.ziggy.ui.config.datareceipt.DataReceiptInstanceDialog;
import gov.nasa.ziggy.ui.config.general.KeyValuePairEditDialog;
import gov.nasa.ziggy.ui.config.group.GroupSelectorDialog;
import gov.nasa.ziggy.ui.config.module.ModuleEditDialog;
import gov.nasa.ziggy.ui.config.parameters.ParamLibImportDialog;
import gov.nasa.ziggy.ui.config.parameters.ParameterClassSelectorDialog;
import gov.nasa.ziggy.ui.config.parameters.ParameterSetEditDialog;
import gov.nasa.ziggy.ui.config.parameters.ParameterSetNewDialog;
import gov.nasa.ziggy.ui.config.pipeline.PipelineEditDialog;
import gov.nasa.ziggy.ui.config.pipeline.PipelineNodeEditDialog;
import gov.nasa.ziggy.ui.config.security.UserEditDialog;
import gov.nasa.ziggy.ui.messaging.ConsoleMessageDispatcher;
import gov.nasa.ziggy.ui.metrilyzer.MetrilyzerPanel;
import gov.nasa.ziggy.ui.mon.master.MasterStatusPanel;
import gov.nasa.ziggy.ui.mon.master.ProcessesIndicatorPanel;
import gov.nasa.ziggy.ui.mon.master.StatusSummaryPanel;
import gov.nasa.ziggy.ui.ops.instances.AlertLogDialog;
import gov.nasa.ziggy.ui.ops.instances.InstanceCostEstimateDialog;
import gov.nasa.ziggy.ui.ops.instances.InstanceDetailsDialog;
import gov.nasa.ziggy.ui.ops.instances.InstanceStatsDialog;
import gov.nasa.ziggy.ui.ops.instances.OpsInstancesPanel;
import gov.nasa.ziggy.ui.ops.instances.RestartDialog;
import gov.nasa.ziggy.ui.ops.instances.TaskInfoDialog;
import gov.nasa.ziggy.ui.ops.instances.TaskLogInformationDialog;
import gov.nasa.ziggy.ui.ops.instances.TextualReportDialog;
import gov.nasa.ziggy.ui.ops.parameters.EditParametersDialog;
import gov.nasa.ziggy.ui.ops.parameters.ParameterSetSelectorDialog;
import gov.nasa.ziggy.ui.ops.parameters.ViewParametersDialog;
import gov.nasa.ziggy.ui.ops.triggers.EditTriggerDialog;
import gov.nasa.ziggy.ui.ops.triggers.FireTriggerDialog;
import gov.nasa.ziggy.ui.ops.triggers.NewTriggerDialog;
import gov.nasa.ziggy.ui.ops.triggers.OpsTriggersPanel;
import gov.nasa.ziggy.ui.ops.triggers.TriggersTreeModel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.CrudProxyExecutor;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;
import gov.nasa.ziggy.util.ZiggyBuild;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.ZiggyVersion;

/**
 * The console GUI.
 * <p>
 * Used by the pipeline operator to configure, launch, and monitor pipelines.
 *
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public class ZiggyGuiConsole extends javax.swing.JFrame {
    private static final Logger log = LoggerFactory.getLogger(ZiggyGuiConsole.class);

    private static final int MAIN_WINDOW_HEIGHT = 900;
    private static final int MAIN_WINDOW_WIDTH = 1400;

    private static final String ZIGGY_LOGO_FILE_NAME = "ziggy-small-clear.png";
    private static final String ZIGGY_LOGO_DIR = "/images/";

    private static ZiggyGuiConsole instance = null;
    public static CrudProxyExecutor crudProxyExecutor = new CrudProxyExecutor();

    public static User currentUser = null;

    private static Image pipelineImage;
    private static Image ziggyImage;

    private JTabbedPane consoleTabbedPane;
    private JPanel statusPanel;
    private JMenuBar consoleMenuBar;
    private JMenuItem helpMenuItem;
    private JMenu helpMenu;
    private JMenuItem exitMenuItem;
    private JMenu fileMenu;
    private JPanel poweredByPanel;
    private JPanel spacerPanel;

    /** Config tab */
    private JPanel configDataPanel;
    private JSplitPane configSplitPane;
    private JScrollPane configDataScrollPane;
    private ConfigTree configTree2;
    private JScrollPane configTreeScrollPane;

    /** Operations tab */
    private JTabbedPane operationsTabbedPane;
    private OpsInstancesPanel opsInstancesPanel;
    private OpsTriggersPanel opsTriggersPanel;

    /** Monitoring tab */
    private JTabbedPane monitoringTabbedPane;
    private StatusSummaryPanel statusSummaryPanel;
    private MetrilyzerPanel metrilyzerPanel;

    {
        // Set Look & Feel
        try {
            MetalLookAndFeel.setCurrentTheme(new SkyBluer());
            javax.swing.UIManager.setLookAndFeel("com.jgoodies.looks.plastic.Plastic3DLookAndFeel");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void launch() {
        try {
            ZiggyBuild.logVersionInfo(log);

            log.info("Initializing Configuration Service");
            ZiggyConfiguration.getInstance();

            log.info("Initializing Database Service");
            try {
                CrudProxy.initialize();
            } catch (Throwable t) {
                log.error("Failed to connect to the database, caught: " + t, t);
                System.exit(-1);
            }

            doLogin();
        } catch (Throwable e) {
            log.error("PipelineConsole.main", e);

            MessageUtil.showError(null, e);
            System.exit(1);
        }

        ZiggyGuiConsole.instance = new ZiggyGuiConsole();
        ZiggyGuiConsole.instance.setVisible(true);
        ZiggyGuiConsole.instance.configSplitPane.setDividerLocation(0.25);
        MasterStatusPanel.setDividerLocation(0.25);

        log.debug("Pipeline console initializeAndStart(): end");

    }

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("USAGE: console");
            System.exit(-1);
        }
        launch();
    }

    private static void doLogin() {
        Configuration config = ZiggyConfiguration.getInstance();
        boolean devModeRequireLogin = config.getBoolean(PropertyNames.REQUIRE_LOGIN_OVERRIDE,
            false);

        boolean requireLogin = devModeRequireLogin || ZiggyVersion.isRelease();

        // Don't require login if there are no configured users.
        if (new UserCrud().retrieveAllUsers().isEmpty()) {
            requireLogin = false;
        }

        UserCrudProxy userCrud = new UserCrudProxy();
        if (requireLogin) {

            currentUser = userCrud.retrieveUser(SystemUtils.USER_NAME);

            if (currentUser == null) {
                log.error("Exceeded max login attempts");
                System.exit(-1);
            }
        }
    }

    private ZiggyGuiConsole() {
        super();
        initGUI();
    }

    private void initGUI() {
        try {
            setTitle("Ziggy Console");
            {
                addWindowListener(new WindowAdapter() {
                    @Override
                    public void windowClosing(WindowEvent evt) {
                        System.exit(0);
                    }
                });
            }
            setSize(MAIN_WINDOW_WIDTH, MAIN_WINDOW_HEIGHT);
            {
                ImageIcon consoleImageIcon = getConsoleIcon();
                setIconImage(consoleImageIcon.getImage());
            }
            {
                consoleMenuBar = new JMenuBar();
                getContentPane().add(getStatusPanel(), BorderLayout.NORTH);
                getContentPane().add(getConsoleTabbedPane(), BorderLayout.CENTER);
                setJMenuBar(consoleMenuBar);
                consoleMenuBar.add(getFileMenu());
                consoleMenuBar.add(getHelpMenu());
            }
            {
                int rmiPort = ZiggyConfiguration.getInstance()
                    .getInt(MessageHandler.RMI_REGISTRY_PORT_PROP,
                        MessageHandler.RMI_REGISTRY_PORT_PROP_DEFAULT);
                log.info("Starting UiCommunicator instance with registry on port " + rmiPort);
                MessageHandler messageHandler = new MessageHandler(
                    new ConsoleMessageDispatcher(MasterStatusPanel.alertMessageTableModel(),
                        MasterStatusPanel.processesStatusPanel(), true));
                UiCommunicator.initializeInstance(messageHandler, rmiPort);
                ZiggyShutdownHook.addShutdownHook(() -> {
                    UiCommunicator.reset();
                });
                log.info("UiCommunicator instance started");
                getOpsInstancesPanel().startAutoRefresh();
                ProcessesIndicatorPanel.addWorkerDataComponents();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JTabbedPane getConsoleTabbedPane() {
        if (consoleTabbedPane == null) {
            consoleTabbedPane = new JTabbedPane();
            consoleTabbedPane.addTab("Configuration", null, getConfigSplitPane(), null);
            consoleTabbedPane.addTab("Operations", null, getOperationsTabbedPane(), null);
            consoleTabbedPane.addTab("Monitoring", null, getMonitoringTabbedPane(), null);
        }
        return consoleTabbedPane;
    }

    private JTabbedPane getOperationsTabbedPane() {
        if (operationsTabbedPane == null) {
            operationsTabbedPane = new JTabbedPane();
            operationsTabbedPane.addTab("Instances", null, getOpsInstancesPanel(), null);
            operationsTabbedPane.addTab("Triggers", null, getOpsTriggersPanel(), null);
        }
        return operationsTabbedPane;
    }

    private JTabbedPane getMonitoringTabbedPane() {
        if (monitoringTabbedPane == null) {
            monitoringTabbedPane = new JTabbedPane();
            monitoringTabbedPane.addTab("Status", null, MasterStatusPanel.masterStatusPanel(),
                null);
            monitoringTabbedPane.addTab("Metrilyzer", null, getMetrilyzerPanel(), null);
        }
        return monitoringTabbedPane;
    }

    private JSplitPane getConfigSplitPane() {
        if (configSplitPane == null) {
            configSplitPane = new JSplitPane();
            configSplitPane.add(getConfigTreeScrollPane(), JSplitPane.LEFT);
            configSplitPane.add(getConfigDataScrollPane(), JSplitPane.RIGHT);
        }
        return configSplitPane;
    }

    private JMenu getFileMenu() {
        if (fileMenu == null) {
            fileMenu = new JMenu();
            fileMenu.setText("File");
            fileMenu.add(getExitMenuItem());
        }
        return fileMenu;
    }

    private JMenuItem getExitMenuItem() {
        if (exitMenuItem == null) {
            exitMenuItem = new JMenuItem();
            exitMenuItem.setText("Exit");
            exitMenuItem.addActionListener(evt -> exitMenuItemActionPerformed());
        }
        return exitMenuItem;
    }

    private JMenu getHelpMenu() {
        if (helpMenu == null) {
            helpMenu = new JMenu();
            helpMenu.setText("Help");
            helpMenu.add(getHelpMenuItem());
        }
        return helpMenu;
    }

    private JMenuItem getHelpMenuItem() {
        if (helpMenuItem == null) {
            helpMenuItem = new JMenuItem();
            helpMenuItem.setText("About");
            helpMenuItem.addActionListener(this::helpMenuItemActionPerformed);
        }
        return helpMenuItem;
    }

    private void exitMenuItemActionPerformed() {
        shutdown();
    }

    private JScrollPane getConfigTreeScrollPane() {
        if (configTreeScrollPane == null) {
            configTreeScrollPane = new JScrollPane();
            configTreeScrollPane.setViewportView(getConfigTree());
        }
        return configTreeScrollPane;
    }

    private ConfigTree getConfigTree() {
        if (configTree2 == null) {
            configTree2 = new ConfigTree((ConfigDataPanel) getConfigDataPanel());
        }
        return configTree2;
    }

    private JScrollPane getConfigDataScrollPane() {
        if (configDataScrollPane == null) {
            configDataScrollPane = new JScrollPane();
            configDataScrollPane.setViewportView(getConfigDataPanel());
        }
        return configDataScrollPane;
    }

    private JPanel getConfigDataPanel() {
        if (configDataPanel == null) {
            configDataPanel = new ConfigDataPanel();
        }
        return configDataPanel;
    }

    private OpsTriggersPanel getOpsTriggersPanel() {
        if (opsTriggersPanel == null) {
            opsTriggersPanel = new OpsTriggersPanel();
        }
        return opsTriggersPanel;
    }

    private OpsInstancesPanel getOpsInstancesPanel() {
        if (opsInstancesPanel == null) {
            opsInstancesPanel = new OpsInstancesPanel();
        }
        return opsInstancesPanel;
    }

    private MetrilyzerPanel getMetrilyzerPanel() {
        if (metrilyzerPanel == null) {
            metrilyzerPanel = new MetrilyzerPanel();
        }
        return metrilyzerPanel;
    }

    private JPanel getStatusPanel() {
        BorderLayout statusPanelLayout = new BorderLayout();
        if (statusPanel == null) {
            statusPanel = new JPanel();
            statusPanel.setLayout(statusPanelLayout);
            statusPanel.add(getPoweredByPanel(), BorderLayout.CENTER);
            statusPanel.add(getStatusSummaryPanel(), BorderLayout.LINE_END);
            statusPanel.add(getSpacerPanel(), BorderLayout.LINE_START);
        }
        float y0 = statusPanelLayout.getLayoutAlignmentY(getPoweredByPanel());
        float y1 = statusPanelLayout.getLayoutAlignmentY(getStatusSummaryPanel());
        log.info("y0 == " + y0 + ", y1 == " + y1);
        return statusPanel;
    }

    private JPanel getPoweredByPanel() {
        if (poweredByPanel == null) {
            poweredByPanel = new JPanel();
            ImageIcon optionalIcon = getPipelineIcon();
            if (optionalIcon != null) {
                JLabel pipelineLabel = new JLabel(scaledImageIcon(optionalIcon),
                    SwingConstants.CENTER);
                poweredByPanel.add(pipelineLabel);
            }
            JLabel poweredBy = new JLabel("powered by");
            poweredByPanel.add(poweredBy);
            JLabel ziggyLabel = new JLabel(scaledImageIcon(getZiggyIcon()), SwingConstants.CENTER);
            poweredByPanel.add(ziggyLabel);
        }
        return poweredByPanel;
    }

    private ImageIcon scaledImageIcon(ImageIcon originalImageIcon) {
        Image originalImage = originalImageIcon.getImage();
        int height = originalImage.getHeight(null);
        int width = originalImage.getWidth(null);
        float scaleFactor = 30F / height;
        float newHeight = height * scaleFactor;
        float newWidth = width * scaleFactor;
        Image scaledImage = originalImage.getScaledInstance((int) newWidth, (int) newHeight,
            Image.SCALE_SMOOTH);
        return new ImageIcon(scaledImage);
    }

    private JPanel getSpacerPanel() {
        if (spacerPanel == null) {
            spacerPanel = new JPanel();
            spacerPanel.setPreferredSize(new Dimension(185, 40));
        }
        return spacerPanel;
    }

    private StatusSummaryPanel getStatusSummaryPanel() {
        if (statusSummaryPanel == null) {
            statusSummaryPanel = new StatusSummaryPanel();
        }
        return statusSummaryPanel;
    }

    private void helpMenuItemActionPerformed(ActionEvent evt) {
        log.debug("helpMenuItem.actionPerformed, event=" + evt);

        AboutDialog about = new AboutDialog(this);
        about.setLocationRelativeTo(this);
        about.setVisible(true);
    }

    private static Image pipelineImage() {
        if (pipelineImage == null) {
            String pipelineImageLocation = ZiggyConfiguration.getInstance()
                .getString(PropertyNames.PIPELINE_LOGO_FILE_PROP_NAME, null);
            if (pipelineImageLocation != null) {
                pipelineImage = getImage(pipelineImageLocation);
            }
        }
        return pipelineImage;
    }

    private static Image ziggyImage() {
        if (ziggyImage == null) {
            ziggyImage = getImage(
                ZiggyGuiConsole.class.getResource(ZIGGY_LOGO_DIR + ZIGGY_LOGO_FILE_NAME));
        }
        return ziggyImage;
    }

    private static Image getImage(String imageLocation) {
        try {
            return getImage(new File(imageLocation).toURI().toURL());
        } catch (MalformedURLException e) {
            log.warn("Bad URL formed from " + imageLocation, e);
            return null;
        }
    }

    private static Image getImage(URL url) {
        Image image = null;
        try {
            image = ImageIO.read(url);
        } catch (IOException e) {
            log.warn("Unable to load image from file " + url.toString(), e);
        }
        return image;
    }

    public static Image pipelineOrZiggyImage() {
        Image image = pipelineImage();
        if (image == null) {
            image = ziggyImage();
        }
        return image;
    }

    private ImageIcon getPipelineIcon() {
        ImageIcon pipelineIcon = null;
        String pipelineImageLocation = ZiggyConfiguration.getInstance()
            .getString(PropertyNames.PIPELINE_LOGO_FILE_PROP_NAME, null);
        if (pipelineImage == null && pipelineImageLocation != null) {
            pipelineImage = getImage(pipelineImageLocation);
        }
        if (pipelineImage != null) {
            pipelineIcon = new ImageIcon(pipelineImage);
        }
        return pipelineIcon;
    }

    private ImageIcon getZiggyIcon() {
        ImageIcon ziggyIcon = null;
        if (ziggyImage == null) {
            ziggyImage = getImage(getClass().getResource(ZIGGY_LOGO_DIR + ZIGGY_LOGO_FILE_NAME));
        }
        if (ziggyImage != null) {
            ziggyIcon = new ImageIcon(ziggyImage);
        }
        return ziggyIcon;
    }

    private ImageIcon getConsoleIcon() {
        return new ImageIcon(pipelineOrZiggyImage());
    }

// Public static methods begin here. These execute a private method of the singleton
// instance.

    public static void reloadConfigTreeModel() {
        instance.getConfigTree().reloadModel();
    }

    public static void shutdown() {
        System.exit(0);
    }

// Factory methods: these are methods that return an instance of a Swing class that has
// the singleton instance as its parent.

    public static KeyValuePairEditDialog newKeyValuePairEditDialog(KeyValuePair keyValuePair) {
        return new KeyValuePairEditDialog(instance, keyValuePair);
    }

    public static GroupSelectorDialog newGroupSelectorDialog() {
        return new GroupSelectorDialog(instance);
    }

    public static ModuleEditDialog newModuleEditDialog(PipelineModuleDefinition module) {
        return new ModuleEditDialog(instance, module);
    }

    public static ParameterClassSelectorDialog newParameterClassSelectorDialog() {
        return new ParameterClassSelectorDialog(instance);
    }

    public static ParameterSetNewDialog newParameterSetNewDialog() {
        return new ParameterSetNewDialog(instance);
    }

    public static ParameterSetEditDialog newParameterSetEditDialog(ParameterSet module,
        boolean isNew) {
        return new ParameterSetEditDialog(instance, module, isNew);
    }

    public static PipelineNodeEditDialog newPipelineNodeEditDialog(PipelineDefinition definition,
        PipelineDefinitionNode node) throws Exception {
        return new PipelineNodeEditDialog(instance, definition, node);
    }

    public static PipelineEditDialog newPipelineEditDialog(PipelineDefinition pipeline) {
        return new PipelineEditDialog(instance, pipeline);
    }

    public static <T> DoubleListDialog<T> newDoubleListDialog(String title,
        String availableListTitle, List<T> availableListContents, String selectedListTitle,
        List<T> selectedListContents) {
        return new DoubleListDialog<>(instance, title, availableListTitle, availableListContents,
            selectedListTitle, selectedListContents);
    }

    public static UserEditDialog newUserEditDialog(User user) {
        return new UserEditDialog(instance, user);
    }

    public static InstanceDetailsDialog newInstanceDetailsDialog(
        PipelineInstance selectedInstance) {
        return new InstanceDetailsDialog(instance, selectedInstance);
    }

    public static TaskLogInformationDialog newTaskLogInformationDialog(PipelineTask selectedTask) {
        return new TaskLogInformationDialog(instance, selectedTask);
    }

    public static EditParametersDialog newEditParametersDialog(ParameterSet parameterSet) {
        return new EditParametersDialog(instance, parameterSet);
    }

    public static ParameterSetSelectorDialog newParameterSetSelectorDialog() {
        return new ParameterSetSelectorDialog(instance);
    }

    public static ParameterSetSelectorDialog newParameterSetSelectorDialog(
        Class<? extends Parameters> filterClass) {
        return new ParameterSetSelectorDialog(instance, filterClass);
    }

    public static ViewParametersDialog newViewParametersDialog(ParameterSet parameterSet) {
        return new ViewParametersDialog(instance, parameterSet);
    }

    public static EditTriggerDialog newEditTriggerDialog(PipelineDefinition clonedTrigger,
        TriggersTreeModel triggersTreeModel) {
        return new EditTriggerDialog(instance, clonedTrigger, triggersTreeModel);
    }

    public static FireTriggerDialog newFireTriggerDialog(PipelineDefinition trigger) {
        return new FireTriggerDialog(instance, trigger);
    }

    public static NewTriggerDialog newNewTriggerDialog() {
        return new NewTriggerDialog(instance);
    }

    public static InstanceCostEstimateDialog newInstanceCostEstimateDialog(
        PipelineInstance pipelineInstance) {
        return new InstanceCostEstimateDialog(instance, pipelineInstance);
    }

    public static DataReceiptInstanceDialog newDataReceiptInstanceDialog(
        DataReceiptInstance dataReceiptInstance) {
        return new DataReceiptInstanceDialog(instance, dataReceiptInstance);
    }

// Show methods: these methods call a Swing show* method in a way that ensures that
// the PipelineConsole singleton instance is the parent of the resulting object.

    public static void showInstanceStatsDialog(PipelineInstance pipelineInstance) {
        InstanceStatsDialog.showInstanceStatsDialog(instance, pipelineInstance);
    }

    public static void showError(Throwable t) {
        MessageUtil.showError(instance, t);
    }

    public static void showMessageDialog(Object message, String title, int messageType) {
        JOptionPane.showMessageDialog(instance, message, title, messageType);
    }

    public static void showAlertLogDialog(long id) {
        AlertLogDialog.showAlertLogDialog(instance, id);
    }

    public static int showConfirmDialog(Object message, String title, int optionType) {
        return JOptionPane.showConfirmDialog(instance, message, title, optionType);
    }

    public static void showTaskInfoDialog(PipelineTask selectedTask) {
        TaskInfoDialog.showTaskInfoDialog(instance, selectedTask);
    }

    public static int showOptionDialog(Object message, String title, int optionType,
        int messageType, Icon icon, Object[] options, Object initialValue) {
        return JOptionPane.showOptionDialog(instance, message, title, optionType, messageType, icon,
            options, initialValue);
    }

    public static void showReport(String report) {
        TextualReportDialog.showReport(instance, report);
    }

    public static String showInputDialog(Object message, String title, int messageType) {
        return JOptionPane.showInputDialog(instance, message, title, messageType);
    }

    public static Object showInputDialog(Object message, String title, int messageType, Icon icon,
        Object[] selectionValues, Object initialSelectionValue) {
        return JOptionPane.showInputDialog(instance, message, title, messageType, icon,
            selectionValues, initialSelectionValue);
    }

// Action methods: these are methods that perform a specific action that requires use
// of a Swing object, and further require that the object have the PipelineConsole
// singleton instance as the parent of the Swing object.

    public static List<String> selectParamSet(List<ParameterSetDescriptor> dryRunResults) {
        return ParamLibImportDialog.selectParamSet(instance, dryRunResults);
    }

    public static RunMode restartTasks(List<PipelineTask> pipelineTasks,
        Map<Long, ProcessingSummary> taskAttrs) {
        return RestartDialog.restartTasks(instance, pipelineTasks, taskAttrs);
    }

    public static Group selectGroup() {
        return GroupsDialog.selectGroup(instance);
    }

    public static void displayOperationsTab() {
        instance.getConsoleTabbedPane().setSelectedComponent(instance.getOperationsTabbedPane());
    }

}
