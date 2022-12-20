package gov.nasa.ziggy.ui.common;

import java.awt.Graphics;
import java.awt.Image;

import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JLabel;

public class ScalableImageLabel extends JLabel {

    private static final long serialVersionUID = 20221220L;

    private Image originalImage;

    @Override
    public void setIcon(Icon icon) {
        super.setIcon(icon);
        if (icon instanceof ImageIcon) {
            originalImage = ((ImageIcon) icon).getImage();
        }
    }

    @Override
    public void paint(Graphics g) {
        int height = getParent().getHeight();
        int width = getParent().getWidth();
        float originalHeight = originalImage.getHeight(null);
        float originalWidth = originalImage.getWidth(null);
        float heightScale = height / originalHeight;
        float widthScale = width / originalWidth;
        float scale = Math.min(heightScale, widthScale);
        scale = Math.min(scale, 1.0F);
        int scaledWidth = (int) (originalWidth * scale);
        int scaledHeight = (int) (originalHeight * scale);
        Image scaledImage = originalImage.getScaledInstance(scaledWidth, scaledHeight,
            Image.SCALE_SMOOTH);
        g.drawImage(scaledImage, 0, 0, null);
    }
}
