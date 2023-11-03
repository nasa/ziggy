package gov.nasa.ziggy.ui.util;

import javax.swing.JLabel;

/**
 * Convenience class for building HTML strings used for setting the text on {@link JLabel}s or other
 * Swing components that support HTML text.
 *
 * @author Todd Klaus
 */
public class HtmlBuilder {
    StringBuilder html = new StringBuilder();

    public HtmlBuilder() {
    }

    public HtmlBuilder(String text) {
        append(text);
    }

    public static HtmlBuilder htmlBuilder() {
        return new HtmlBuilder();
    }

    public static HtmlBuilder htmlBuilder(String text) {
        return new HtmlBuilder(text);
    }

    /**
     * Wraps the given strings or other objects in {@code <html>}. This method is useful if the
     * {@code append()} methods aren't needed.
     */
    public static String toHtml(Object... strings) {
        StringBuilder htmlString = new StringBuilder("<html>");
        for (Object s : strings) {
            htmlString.append(s);
        }
        htmlString.append("</html>");
        return htmlString.toString();
    }

    public HtmlBuilder append(Object text) {
        html.append(text);
        return this;
    }

    public HtmlBuilder appendBold(String text) {
        html.append("<b>").append(text).append("</b>");
        return this;
    }

    public HtmlBuilder appendItalic(String text) {
        html.append("<i>").append(text).append("</i>");
        return this;
    }

    public HtmlBuilder appendColor(String text, String color) {
        html.append("<font color=").append(color).append(">").append(text).append("</font>");
        return this;
    }

    public HtmlBuilder appendBoldColor(String text, String color) {
        html.append("<b>");
        appendColor(text, color);
        html.append("</b>");
        return this;
    }

    public HtmlBuilder appendSize(String text, int size) {
        html.append("<font size=").append(size).append(">").append(text).append("</font>");
        return this;
    }

    public HtmlBuilder appendBreak() {
        html.append("<br/>");
        return this;
    }

    public HtmlBuilder appendCustomHtml(String htmlPrefix, String text, String htmlSuffix) {
        html.append(htmlPrefix);
        html.append(text);
        html.append(htmlSuffix);
        return this;
    }

    @Override
    public String toString() {
        return HtmlBuilder.toHtml(html.toString());
    }
}
