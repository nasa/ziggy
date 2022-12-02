package gov.nasa.ziggy.ui.common;

import javax.swing.JTextField;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;
import javax.swing.text.PlainDocument;

/**
 * Extension of {@link JTextField} which only accepts input that is formatted according to an
 * appropriate filter.
 *
 * @author PT
 */
public abstract class FilteredTextField extends JTextField {

    private static final long serialVersionUID = 20210811L;

    public FilteredTextField() {
        super();
    }

    public FilteredTextField(Document doc, String text, int columns) {
        super(doc, text, columns);
        setFilter();
    }

    public FilteredTextField(int columns) {
        super(columns);
        setFilter();
    }

    public FilteredTextField(String text) {
        super(text);
        setFilter();
    }

    public FilteredTextField(String text, int columns) {
        super(text, columns);
        setFilter();
    }

    private void setFilter() {
        PlainDocument doc = (PlainDocument) getDocument();
        doc.setDocumentFilter(filter());
    }

    protected abstract DocumentFilter filter();

}
