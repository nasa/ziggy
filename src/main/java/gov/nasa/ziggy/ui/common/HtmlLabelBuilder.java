package gov.nasa.ziggy.ui.common;

import javax.swing.JLabel;

/**
 * Convenience class for building HTML strings used for setting the text on {@link JLabel}s or other
 * Swing components that support HTML text.
 *
 * @author Todd Klaus
 */
public class HtmlLabelBuilder {
    StringBuilder html = new StringBuilder();

    public HtmlLabelBuilder() {
    }

    public HtmlLabelBuilder(String text) {
        append(text);
    }

    public HtmlLabelBuilder append(String text) {
        html.append(text);
        return this;
    }

    public HtmlLabelBuilder appendBold(String text) {
        html.append("<b>");
        html.append(text);
        html.append("</b>");
        return this;
    }

    public HtmlLabelBuilder appendItalic(String text) {
        html.append("<i>");
        html.append(text);
        html.append("</i>");
        return this;
    }

    public HtmlLabelBuilder appendColor(String text, String color) {
        html.append("<font color=" + color + ">");
        html.append(text);
        html.append("</font>");
        return this;
    }

    public HtmlLabelBuilder appendSizeModifier(String text, String size) {
        html.append("<font size=" + size + ">");
        html.append(text);
        html.append("</font>");
        return this;
    }

    public HtmlLabelBuilder appendCustomHtml(String htmlPrefix, String text, String htmlSuffix) {
        html.append(htmlPrefix);
        html.append(text);
        html.append(htmlSuffix);
        return this;
    }

    @Override
    public String toString() {
        return "<html>" + html.toString() + "</html>";
    }
}
