package gov.nasa.ziggy.ui.mon.master;

import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * A special {@link IndicatorPanel} that has an {@link Indicator} for each indicator type
 * (processes, workers, pipeline instances, metrics). These parent indicators indicate the health of
 * the worst child indicator of that type (rollup status) and act as a navigation tool (clicking on
 * the parent indicator will display the IndicatorPanel for that type).
 *
 * @author Todd Klaus
 */
public class ParentIndicatorPanel extends IndicatorPanel {
    private static final long serialVersionUID = -2448349084476505804L;

    public ParentIndicatorPanel() {
        super(null);
    }

    public ParentIndicatorPanel(int numRows, boolean hasTitleButtonBar) {
        super(null, numRows, hasTitleButtonBar);
    }

    @Override
    public void update(StatusMessage statusMessage) {
    }

    @Override
    public void dismissAll() {
        // do nothing, parents are static
    }
}
