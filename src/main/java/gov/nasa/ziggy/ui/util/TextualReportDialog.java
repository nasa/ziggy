package gov.nasa.ziggy.ui.util;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import gov.nasa.ziggy.util.io.FileUtil;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TextualReportDialog extends javax.swing.JDialog {

    private String report;
    private String title;
    private Path reportSavePath;

    public TextualReportDialog(Window owner, String report, String title, Path reportSavePath) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.report = report;
        this.title = title;
        this.reportSavePath = reportSavePath;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    public static TextualReportDialog showReport(Window owner, String report, String title) {
        return showReport(owner, report, title, null);
    }

    public static TextualReportDialog showReport(Window owner, String report, String title,
        Path savePath) {
        TextualReportDialog dialog = new TextualReportDialog(owner, report, title, savePath);
        dialog.setVisible(true);
        return dialog;
    }

    private void buildComponent() {
        setTitle(title);

        getContentPane().add(new JScrollPane(getReportTextArea()), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(SAVE, this::save), createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        pack();
    }

    private JTextArea getReportTextArea() {
        JTextArea reportTextArea = new JTextArea();
        reportTextArea.setText(report);
        reportTextArea.setFont(new java.awt.Font("Monospaced", 0, 12));

        return reportTextArea;
    }

    private void save(ActionEvent evt) {

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

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(
            new TextualReportDialog((JFrame) null, "hello!", "Report title", null));
    }
}
