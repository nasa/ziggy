package gov.nasa.ziggy.pipeline;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.text.TextStringBuilder;

/**
 * @author Todd Klaus
 */
public class TriggerValidationResults {
    private final List<String> errors = new LinkedList<>();

    public boolean hasErrors() {
        return errors.size() > 0;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean addError(String error) {
        return errors.add(error);
    }

    public void clearErrors() {
        errors.clear();
    }

    public String errorReport() {
        return errorReport("");
    }

    /**
     * @param indent
     * @return
     */
    public String errorReport(String indent) {
        TextStringBuilder sb = new TextStringBuilder();

        for (String error : errors) {
            sb.append(indent + error);
            sb.appendNewLine();
        }
        return sb.toString();
    }
}
