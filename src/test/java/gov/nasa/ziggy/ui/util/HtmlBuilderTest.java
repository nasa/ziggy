package gov.nasa.ziggy.ui.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HtmlBuilderTest {

    @Test
    public void testAppend() {
        assertEquals("<html>foo bar</html>", new HtmlBuilder("foo ").append("bar").toString());
    }

    @Test
    public void testAppendBold() {
        assertEquals("<html>foo <b>bar</b></html>",
            new HtmlBuilder("foo ").appendBold("bar").toString());
    }

    @Test
    public void testAppendItalic() {
        assertEquals("<html>foo <i>bar</i></html>",
            new HtmlBuilder("foo ").appendItalic("bar").toString());
    }

    @Test
    public void testAppendColor() {
        assertEquals("<html>foo <font color=red>bar</font></html>",
            new HtmlBuilder("foo ").appendColor("bar", "red").toString());
    }

    @Test
    public void testAppendBoldColor() {
        assertEquals("<html>foo <b><font color=red>bar</font></b></html>",
            new HtmlBuilder("foo ").appendBoldColor("bar", "red").toString());
    }

    @Test
    public void testAppendSize() {
        assertEquals("<html>foo <font size=10>bar</font></html>",
            new HtmlBuilder("foo ").appendSize("bar", 10).toString());
    }

    @Test
    public void testAppendBreak() {
        assertEquals("<html>foo <br/>bar</html>",
            new HtmlBuilder("foo ").appendBreak().append("bar").toString());
    }

    @Test
    public void testAppendCustomHtml() {
        assertEquals("<html>foo <p>bar</p></html>",
            new HtmlBuilder("foo ").appendCustomHtml("<p>", "bar", "</p>").toString());
    }

    @Test
    public void testBuilderFactory() {
        assertEquals("<html>foo bar</html>",
            HtmlBuilder.htmlBuilder().append("foo bar").toString());
        assertEquals("<html>foo bar</html>",
            HtmlBuilder.htmlBuilder("foo ").append("bar").toString());
    }
}
