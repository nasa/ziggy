package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TextualReportDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(TextualReportDialog.class);

    private JScrollPane reportTextScrollPane;
    private JButton saveButton;
    private JTextArea reportTextArea;
    private JButton closeButton;
    private JPanel actionPanel;
    private String report = "";
    private Path reportSavePath;

    public TextualReportDialog(JFrame frame, String report) {
        super(frame, true);
        this.report = report;

        initGUI();
    }

    public TextualReportDialog(JDialog dialog, String report) {
        super(dialog, true);
        this.report = report;

        initGUI();
    }

    public static TextualReportDialog showReport(JDialog parent, String report, Path savePath) {
        TextualReportDialog dialog = new TextualReportDialog(parent, report);
        if (savePath != null) {
            dialog.setReportSavePath(savePath);
        }
        dialog.setVisible(true);
        return dialog;
    }

    public static TextualReportDialog showReport(JDialog parent, String report) {
        return showReport(parent, report, null);
    }

    public static TextualReportDialog showReport(JFrame parent, String report) {
        TextualReportDialog dialog = new TextualReportDialog(parent, report);

        dialog.setVisible(true);
        return dialog;
    }

    public void setReportSavePath(Path reportSavePath) {
        log.debug("Report save path: " + reportSavePath.toString());
        this.reportSavePath = reportSavePath;
    }

    private void saveButtonActionPerformed(ActionEvent evt) {
        log.debug("saveButton.actionPerformed, event=" + evt);

        Path savePath = reportSavePath != null ? reportSavePath : savePathFromChooser();
        if (savePath != null) {
            try {
                Files.createDirectories(savePath.getParent());
            } catch (Exception e) {
                MessageUtil.showError(this, e);
                return;
            }
            File file = savePath.toFile();
            try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), FileUtil.ZIGGY_CHARSET))) {
                writer.write(report);
                setTitle(savePath.getFileName().toString());
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    private Path savePathFromChooser() {
        Path selectedPath = null;
        try {
            JFileChooser fc = new JFileChooser();
            int returnVal = fc.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                selectedPath = fc.getSelectedFile().toPath();
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
        return selectedPath;
    }

    private void closeButtonActionPerformed() {
        setVisible(false);
    }

    private void initGUI() {
        try {
            getContentPane().add(getReportTextScrollPane(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            this.setSize(615, 677);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getReportTextScrollPane() {
        if (reportTextScrollPane == null) {
            reportTextScrollPane = new JScrollPane();
            reportTextScrollPane.setViewportView(getReportTextArea());
        }
        return reportTextScrollPane;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(100);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getSaveButton());
            actionPanel.add(getCloseButton());
        }
        return actionPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed());
        }
        return closeButton;
    }

    private JTextArea getReportTextArea() {
        if (reportTextArea == null) {
            reportTextArea = new JTextArea();
            reportTextArea.setText(report);
            reportTextArea.setFont(new java.awt.Font("Monospaced", 0, 12));
        }
        return reportTextArea;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JDialog d = new JDialog();
            TextualReportDialog inst = new TextualReportDialog(d, "hello!");
            inst.setVisible(true);
        });
    }

    private JButton getSaveButton() {
        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("save to file");
            saveButton.addActionListener(this::saveButtonActionPerformed);
        }
        return saveButton;
    }
}
