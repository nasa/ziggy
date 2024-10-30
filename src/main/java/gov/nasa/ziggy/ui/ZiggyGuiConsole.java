package gov.nasa.ziggy.ui;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ABOUT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXIT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.FILE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.HELP;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenu;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messages.ShutdownMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesRequest;
import gov.nasa.ziggy.services.messaging.HeartbeatManager;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;
import gov.nasa.ziggy.ui.status.StatusSummaryPanel;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.util.Requestor;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * The console GUI.
 * <p>
 * Used by the pipeline operator to configure, launch, and monitor pipelines.
 *
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public class ZiggyGuiConsole extends javax.swing.JFrame implements Requestor {
    private static final Logger log = LoggerFactory.getLogger(ZiggyGuiConsole.class);

    public static final String NAME = "Ziggy Console";

    private static final String ZIGGY_LOGO_FILE_NAME = "ziggy-small-clear.png";
    private static final String ZIGGY_LOGO_DIR = "/images/";

    private static Image pipelineImage;
    private static Image ziggyImage;

    private static WorkerResources defaultResources;

    private final UUID uuid = UUID.randomUUID();

    {
        ZiggySwingUtils.setLookAndFeel();
    }

    private ZiggyGuiConsole() {

        // Initialize the ProcessHeartbeatManager for this process.
        log.info("Initializing ProcessHeartbeatManager");
        HeartbeatManager.startInstance();
        log.info("Initializing ProcessHeartbeatManager...done");

        ZiggyMessenger.subscribe(ShutdownMessage.class, message -> {
            log.info("Shutting down due to receipt of shutdown message");
            shutdown();
        });

        ZiggyMessenger.subscribe(WorkerResourcesMessage.class, message -> {
            if (message.getDefaultResources() != null && defaultResources == null) {
                defaultResources = message.getDefaultResources();
            }
        });

        int rmiPort = ZiggyConfiguration.getInstance()
            .getInt(PropertyName.SUPERVISOR_PORT.property(), ZiggyRmiServer.RMI_PORT_DEFAULT);
        log.info("Starting ZiggyRmiClient instance with registry on port {}", rmiPort);
        ZiggyRmiClient.start(NAME);
        ZiggyShutdownHook.addShutdownHook(() -> {
            ZiggyRmiClient.reset();
        });
        log.info("Starting ZiggyRmiClient instance with registry on port {}...done", rmiPort);

        buildComponent();

        ZiggyMessenger.publish(new WorkerResourcesRequest());
        ZiggyMessenger.publish(new InvalidateConsoleModelsMessage(), false);
    }

    public static void launch() {
        try {
            log.info("Starting Ziggy Console ({})", BuildInfo.ziggyVersion());

            ZiggyConfiguration.logJvmProperties();

            ZiggyGuiConsole instance = new ZiggyGuiConsole();
            instance.setLocationByPlatform(true);
            instance.setVisible(true);
        } catch (Throwable e) {
            MessageUtils.showError(null, e);
            System.exit(1);
        }

        log.debug("Ziggy Console initialization complete");
    }

    private void buildComponent() {
        setTitle(NAME);
        setSize(ZiggyGuiConstants.MAIN_WINDOW_WIDTH, (int) (ZiggyGuiConstants.MAIN_WINDOW_WIDTH
            / ZiggyGuiConstants.MAIN_WINDOW_ASPECT_RATIO));
        setIconImage(new ImageIcon(pipelineOrZiggyImage()).getImage());

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent evt) {
                System.exit(0);
            }
        });

        JMenuBar consoleMenuBar = new JMenuBar();
        consoleMenuBar.add(createMenu(FILE, createMenuItem(exitAction())));
        consoleMenuBar.add(createMenu(HELP, createMenuItem(aboutAction())));
        setJMenuBar(consoleMenuBar);

        getContentPane().add(getStatusPanel(), BorderLayout.NORTH);
        getContentPane().add(new ZiggyConsolePanel(), BorderLayout.CENTER);
    }

    private Action exitAction() {
        Action exitAction = new AbstractAction(EXIT, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    shutdown();
                } catch (Exception e) {
                    MessageUtils.showError(ZiggyGuiConsole.this, e);
                }
            }
        };
        exitAction.putValue(Action.ACCELERATOR_KEY,
            KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.CTRL_MASK));
        return exitAction;
    }

    private void shutdown() {
        System.exit(0);
    }

    private Action aboutAction() {
        return new AbstractAction(ABOUT, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                JOptionPane.showMessageDialog(ZiggyGuiConsole.this, content(), "About Ziggy",
                    JOptionPane.PLAIN_MESSAGE);
            }

            private String content() {
                return HtmlBuilder.htmlBuilder()
                    .append(
                        "Ziggy, a portable, scalable infrastructure for science data processing pipelines")
                    .append("<br/><br/>Software version: ")
                    .append(BuildInfo.ziggyVersion())
                    .append("<br/>URL: ")
                    .append("https://github.com/nasa/ziggy")
                    .toString();
            }
        };
    }

    private JPanel getStatusPanel() {
        JPanel strut = getSpacerPanel();
        JPanel poweredByPanel = getPoweredByPanel();
        StatusSummaryPanel statusSummaryPanel = new StatusSummaryPanel();

        JPanel statusPanel = new JPanel();
        GroupLayout statusPanelLayout = new GroupLayout(statusPanel);
        statusPanel.setLayout(statusPanelLayout);

        statusPanelLayout.setHorizontalGroup(statusPanelLayout.createSequentialGroup()
            .addGroup(statusPanelLayout.createParallelGroup(Alignment.LEADING).addComponent(strut))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addGroup(statusPanelLayout.createParallelGroup(Alignment.CENTER)
                .addComponent(poweredByPanel))
            .addPreferredGap(ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(statusPanelLayout.createParallelGroup(Alignment.TRAILING)
                .addComponent(statusSummaryPanel, GroupLayout.PREFERRED_SIZE,
                    GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)));

        statusPanelLayout.setVerticalGroup(statusPanelLayout.createParallelGroup(Alignment.CENTER)
            .addComponent(strut)
            .addComponent(poweredByPanel)
            .addComponent(statusSummaryPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        return statusPanel;
    }

    private JPanel getPoweredByPanel() {
        JPanel poweredByPanel = new JPanel();
        ImageIcon optionalIcon = getPipelineIcon();
        if (optionalIcon != null) {
            JLabel pipelineLabel = new JLabel(scaledImageIcon(optionalIcon), SwingConstants.CENTER);
            poweredByPanel.add(pipelineLabel);
        }
        JLabel poweredBy = new JLabel("powered by");
        poweredByPanel.add(poweredBy);
        JLabel ziggyLabel = new JLabel(scaledImageIcon(getZiggyIcon()), SwingConstants.CENTER);
        poweredByPanel.add(ziggyLabel);
        return poweredByPanel;
    }

    private ImageIcon getPipelineIcon() {
        ImageIcon pipelineIcon = null;
        String pipelineImageLocation = ZiggyConfiguration.getInstance()
            .getString(PropertyName.PIPELINE_LOGO_FILE.property(), null);
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

    private ImageIcon scaledImageIcon(ImageIcon originalImageIcon) {
        Image originalImage = originalImageIcon.getImage();
        int height = originalImage.getHeight(null);
        int width = originalImage.getWidth(null);
        float scaleFactor = 30F / height;
        Image scaledImage = originalImage.getScaledInstance((int) (width * scaleFactor),
            (int) (height * scaleFactor), Image.SCALE_SMOOTH);
        return new ImageIcon(scaledImage);
    }

    private JPanel getSpacerPanel() {
        JPanel spacerPanel = new JPanel();
        spacerPanel.setPreferredSize(new Dimension(140, 0));
        return spacerPanel;
    }

    public static Image pipelineOrZiggyImage() {
        Image image = pipelineImage();
        if (image == null) {
            image = ziggyImage();
        }
        return image;
    }

    private static Image pipelineImage() {
        if (pipelineImage == null) {
            String pipelineImageLocation = ZiggyConfiguration.getInstance()
                .getString(PropertyName.PIPELINE_LOGO_FILE.property(), null);
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
            log.warn("Bad URL formed from {}", imageLocation, e);
            return null;
        }
    }

    private static Image getImage(URL url) {
        Image image = null;
        try {
            image = ImageIO.read(url);
        } catch (IOException e) {
            log.warn("Unable to load image from file {}", url.toString(), e);
        }
        return image;
    }

    public static WorkerResources defaultResources() {
        return defaultResources;
    }

    @Override
    public UUID requestorIdentifier() {
        return uuid;
    }
}
