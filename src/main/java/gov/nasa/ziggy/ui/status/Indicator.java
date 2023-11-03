package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingUtilities;
import javax.swing.border.BevelBorder;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * Represents a status information object that is a subclass of {@link JPanel}. The object can
 * include text information that provides detailed information, as well as a {@link State} with the
 * following values and associated colors:
 * <ol>
 * <li>{@link State#IDLE}: the current state of the object being reported is inactive (gray).
 * <li>{@link State#NORMAL}: the current state of the object being reported is good (green).
 * <li>{@link State#WARNING}: the current state of the object being reported has a non-fatal warning
 * (amber).
 * <li>{@link State#ERROR}: the current state of the object being reported has a fatal error (red).
 * </ol>
 * The {@link Indicator} also supports user-supplied tool tip text. The text can be manually changed
 * on its own, or it can be changed when the state is changed. The user may also choose to supply a
 * static tool tip text {@link String} for each of the {@link State#IDLE} and {@link State#NORMAL}
 * states. These will be used when the appropriate state is assigned to the {@link Indicator}.
 * <p>
 * In addition, each {@link Indicator} may also be paired with an instance of {@link IdiotLight}.
 * The {@link IdiotLight} class provides a less obtrusive and less informative summary of the object
 * state that's suitable for use on the pipeline console.
 *
 * @author PT
 * @author Bill Wohler
 */
public class Indicator extends JPanel {

    private static final long serialVersionUID = 20230822L;

    /** Preferred width of the indicator. */
    private static final int WIDTH = 200;

    /** Preferred height of the full indicator. */
    private static final int HEIGHT = 75;

    /**
     * Preferred height of the single line indicator, which is the height of a JLabel component plus
     * a little padding to keep the items from running into each other in a list.
     */
    private static final int SINGLE_LINE_HEIGHT = ZiggySwingUtils.textHeight(new JLabel()) + 3;

    /** Percentage of the width that the state color bar should take. */
    private static final double STATE_WIDTH_RATIO = 0.5;

    public enum State {
        NORMAL(new java.awt.Color(0, 200, 0)),
        WARNING(new java.awt.Color(250, 250, 0)),
        ERROR(new java.awt.Color(250, 0, 0)),
        IDLE(Color.GRAY);

        private Color color;

        State(Color color) {
            this.color = color;
        }

        public Color getColor() {
            return color;
        }
    }

    private String displayName;
    private JLabel name;
    private State state = State.NORMAL;
    private JPanel statePanel;
    private String normalStateToolTipText;
    private String idleStateToolTipText;
    private JPanel infoPanel;
    private IdiotLight idiotLight;

    private GroupLayout dataPanelLayout;
    private Set<IndicatorListener> listeners = new HashSet<>();

    public Indicator(String displayName) {
        this.displayName = displayName;
        buildComponent();
    }

    public void addIndicatorListener(IndicatorListener listener) {
        listeners.add(listener);
    }

    private void buildComponent() {
        setPreferredSize(new java.awt.Dimension(WIDTH, HEIGHT));
        setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
        setVerifyInputWhenFocusTarget(false);

        createDataPanel(displayName);

        setState(state);
    }

    /** Make the indicator short and not bold for inclusion in a list. */
    public void singleLine() {
        JLabel oldName = name;
        name = new JLabel(displayName);
        dataPanelLayout.replace(oldName, name);
        setPreferredSize(new Dimension(WIDTH, SINGLE_LINE_HEIGHT));
        setBorder(BorderFactory.createEmptyBorder());
    }

    private void createDataPanel(String displayName) {
        name = new JLabel(htmlBuilder().appendBold(displayName).toString());

        // Setting the height to 90% of the line height keeps the state bars in lists from bleeding
        // together.
        statePanel = new JPanel();
        statePanel.setPreferredSize(
            new Dimension((int) (WIDTH * STATE_WIDTH_RATIO), (int) (SINGLE_LINE_HEIGHT * 0.90)));
        statePanel.setBackground(new java.awt.Color(0, 167, 0));
        statePanel.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));

        infoPanel = new JPanel();
        infoPanel.setLayout(new GridLayout(3, 2));
        infoPanel.setBackground(new java.awt.Color(255, 255, 255));

        dataPanelLayout = new GroupLayout(this);
        setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addComponent(name)
                .addPreferredGap(ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE,
                    Short.MAX_VALUE)
                .addComponent(statePanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE))
            .addComponent(infoPanel));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(name)
                .addComponent(statePanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE))
            .addComponent(infoPanel));
    }

    public static State summaryState(Indicator... indicators) {
        State sumState = State.IDLE;
        for (Indicator indicator : indicators) {
            if (indicator == null) {
                continue;
            }
            if (indicator.getState() == State.NORMAL && sumState == State.IDLE) {
                sumState = State.NORMAL;
            }
            if (indicator.getState() == State.WARNING
                && (sumState == State.IDLE || sumState == State.NORMAL)) {
                sumState = State.WARNING;
            }
            if (indicator.getState() == State.ERROR) {
                sumState = State.ERROR;
            }
        }
        return sumState;
    }

    public static String summaryToolTipText(Indicator... indicators) {
        List<String> stateStrings = new ArrayList<>();
        for (Indicator indicator : indicators) {
            if (indicator == null) {
                continue;
            }
            if (indicator.getState() == State.WARNING || indicator.getState() == State.ERROR) {
                stateStrings.add(indicator.getToolTipText());
            }
        }
        if (stateStrings.size() == 0) {
            return null;
        }
        String summaryString = null;
        for (int i = 0; i < stateStrings.size(); i++) {
            summaryString += stateStrings.get(i);
            if (i < stateStrings.size() - 1) {
                summaryString += System.lineSeparator();
            }
        }
        return summaryString;
    }

    /**
     * Sets the state of the {@link Indicator} to a specified value. If the state is
     * {@link State#NORMAL} or {@link State#IDLE}, the appropriate tooltip text is also applied. The
     * color and tooltip text are applied to the {@link Indicator} and its {@link IdiotLight}, if
     * any.
     */
    public void setState(State state) {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> setState(state));
            return;
        }

        this.state = state;
        statePanel.setBackground(state.getColor());

        if (state == State.NORMAL && normalStateToolTipText != null) {
            setToolTipText(normalStateToolTipText);
        }
        if (state == State.IDLE && idleStateToolTipText != null) {
            setToolTipText(idleStateToolTipText);
        }

        if (idiotLight != null) {
            idiotLight.setState(state, getToolTipText());
        }

        for (IndicatorListener listener : listeners) {
            listener.stateChanged();
        }
    }

    /**
     * Sets the state of the {@link Indicator} to a specified value, and sets the tooltip text to a
     * specified {@link String}. If the state is {@link State#NORMAL} or {@link State#IDLE}, and the
     * tooltip text for these states is set, then the tooltip text argument to this method will be
     * ignored. The color and tooltip text are applied to the {@link Indicator} and its
     * {@link IdiotLight}, if any.
     */
    public void setState(State state, String toolTipText) {
        setToolTipText(toolTipText);
        setState(state);
    }

    public State getState() {
        return state;
    }

    public void addDataComponent(Component component) {
        infoPanel.add(component);
    }

    public IdiotLight getIdiotLight() {
        return idiotLight;
    }

    public void setIdiotLight(IdiotLight idiotLight) {
        this.idiotLight = idiotLight;
    }

    @Override
    public void setToolTipText(String toolTipText) {
        super.setToolTipText(toolTipText);
        if (idiotLight != null) {
            idiotLight.setToolTipText(toolTipText);
        }
    }

    public void setNormalStateToolTipText(String normalStateToolTipText) {
        this.normalStateToolTipText = normalStateToolTipText;
    }

    public void setIdleStateToolTipText(String idleStateToolTipText) {
        this.idleStateToolTipText = idleStateToolTipText;
    }

    /**
     * Visual representation of one of the status summary panel idiot lights, in this case a filled
     * circle of one of the standard solid colors.
     *
     * @author PT
     */
    public static class IdiotLight extends JPanel {

        private static final long serialVersionUID = 20230511L;
        private static final int LIGHT_SIZE = 12;
        private State state = State.NORMAL;

        public IdiotLight() {
            Dimension size = new Dimension(LIGHT_SIZE, LIGHT_SIZE + 10);
            setPreferredSize(size);
            setMinimumSize(size);
            setMaximumSize(size);
            setSize(size);
            setLayout(null);
        }

        public void setState(State state, String toolTipText) {
            if (this.state != state) {
                this.state = state;
                repaint();
            }
            setToolTipText(toolTipText);
        }

        public State getState() {
            return state;
        }

        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            g.setColor(state.getColor());
            g.fillOval(0, 5, LIGHT_SIZE - 1, LIGHT_SIZE - 1);
        }
    }

    /**
     * The listener interface for receiving "interesting" changes to an indicator (state change).
     * <p>
     * The listener object is registered with an indicator using the {@code addMouseListener}
     * method. When a change in the indicator occurs, the relevant method in the listener object is
     * invoked.
     */
    interface IndicatorListener {
        /**
         * Invoked when this indicator's state changes.
         * <p>
         * If it is needed in the future, an {@code IndicatorEvent} parameter can be added that
         * would include the source {@code Indicator} and the old and new state.
         */
        void stateChanged();
    }
}
