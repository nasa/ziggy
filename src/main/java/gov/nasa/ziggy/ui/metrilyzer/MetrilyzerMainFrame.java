package gov.nasa.ziggy.ui.metrilyzer;

import java.awt.BorderLayout;
import java.io.File;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.metrics.MetricsFileParser;

/**
 * Frame and main() for running the Metrilyzer as a standalone app.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class MetrilyzerMainFrame extends javax.swing.JFrame {
    private static final Logger log = LoggerFactory.getLogger(MetrilyzerMainFrame.class);

    private MetrilyzerPanel metrilyzerPanel = null;
    private final MetricsFileParser metricsFileParser;

    /**
     * @param metricsFileParser this may be null
     */
    public MetrilyzerMainFrame(MetricsFileParser metricsFileParser) {
        this.metricsFileParser = metricsFileParser;
        initGUI();
    }

    private void initGUI() {
        setTitle("Metrics Analysis Tool");
        BorderLayout thisLayout = new BorderLayout();
        getContentPane().setLayout(thisLayout);
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        getContentPane().add(getMetrilyzerPanel(), BorderLayout.CENTER);
        pack();
        setSize(1024, 768);
    }

    private MetrilyzerPanel getMetrilyzerPanel() {
        if (metrilyzerPanel == null) {
            if (metricsFileParser == null) {
                metrilyzerPanel = new MetrilyzerPanel();
            } else {
                metrilyzerPanel = new MetrilyzerPanel(metricsFileParser);
            }
        }
        return metrilyzerPanel;
    }

    /**
     * Main method to display this JFrame.
     */
    public static void main(String[] args) {
        File metricsFile = null;
        if (args.length != 0) {
            metricsFile = new File(args[0]);
        }

        final MetricsFileParser metricsFileParser = metricsFile == null ? null
            : new MetricsFileParser(metricsFile);

        try {
            SwingUtilities.invokeAndWait(() -> {
                try {
                    // PlasticLookAndFeel.setMyCurrentTheme(new SkyBluer());
                    // javax.swing.UIManager.setLookAndFeel("com.jgoodies.looks.plastic.Plastic3DLookAndFeel");

                    for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                        if ("Nimbus".equals(info.getName())) {
                            UIManager.setLookAndFeel(info.getClassName());
                            break;
                        }
                    }
                    MetrilyzerMainFrame mf = new MetrilyzerMainFrame(metricsFileParser);
                    mf.setVisible(true);
                } catch (Exception e) {
                    log.error("main", e);
                }
            });
        } catch (Throwable t) {
            log.error("This error is lame.", t);
        }
    }
}
