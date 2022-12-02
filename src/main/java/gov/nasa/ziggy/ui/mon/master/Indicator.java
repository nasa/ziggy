package gov.nasa.ziggy.ui.mon.master;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;
import javax.swing.border.BevelBorder;

/**
 * Represents a status information object that is a subclass of {@link JPanel}. The object can
 * include text information that provides detailed information, as well as a color-coded
 * {@link State} with the following values:
 * <ol>
 * <li>GRAY: the current state of the object being reported is inactive.
 * <li>GREEN: the current state of the object being reported is good.
 * <li>AMBER: the current state of the object being reported has a non-fatal warning.
 * <li>RED: the current state of the object being reported has a fatal error.
 * </ol>
 * The {@link Indicator} also supports user-supplied tool tip text. The text can be manually changed
 * on its own, or it can be changed when the state is changed. The user may also choose to supply a
 * static tool tip text {@link String} for each of the GRAY and GREEN states. These will be used
 * when the appropriate state is assigned to the {@link Indicator}.
 * <p>
 * In addition, each {@link Indicator} may also be paired with an instance of {@link IdiotLight}.
 * The {@link IdiotLight} class provides a less obtrusive and less informative summary of the object
 * state that's suitable for use on the pipeline console.
 *
 * @author PT
 */
public class Indicator extends javax.swing.JPanel {
    private static final long serialVersionUID = 20220510L;

    public enum State {
        GREEN(new java.awt.Color(0, 200, 0)),
        AMBER(new java.awt.Color(250, 250, 0)),
        RED(new java.awt.Color(250, 0, 0)),
        GRAY(Color.GRAY);

        private Color color;

        State(Color color) {
            this.color = color;
        }

        public Color getColor() {
            return color;
        }
    }

    /**
     * should be globally unique among all {@link Indicator}s. Used for storing indicators in
     * collections, etc.
     */
    private String id;
    private String indicatorDisplayName = "name";
    private State state = State.GREEN;

    private final List<IndicatorListener> listeners = new LinkedList<>();
    private JMenuItem dismissMenuItem;
    private JPopupMenu popupMenu;

    private JPanel topPanel;
    private JPanel statePanel;
    private JPanel dataPanel;
    private JLabel namelabel;
    private IdiotLight idiotLight;
    private String toolTipContent;
    private String greenStateToolTipText;
    private String grayStateToolTipText;

    private IndicatorPanel parentIndicatorPanel;

    private long lastUpdated = System.currentTimeMillis();

    public Indicator() {
        initGUI();
    }

    public Indicator(IndicatorPanel parentIndicatorPanel, String displayName) {
        this.parentIndicatorPanel = parentIndicatorPanel;
        indicatorDisplayName = displayName;
        initGUI();
    }

    /**
     * Sets the state of the {@link Indicator} to a specified value. If the state is GREEN or GRAY,
     * the appropriate tooltip text is also applied. The color and tooltip text are applied to the
     * {@link Indicator} and its {@link IdiotLight}, if any.
     */
    public void setState(State state) {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> setState(state));
            return;
        }
        this.state = state;
        if (state == State.GREEN && greenStateToolTipText != null) {
            toolTipContent = greenStateToolTipText;
        }
        if (state == State.GRAY && grayStateToolTipText != null) {
            toolTipContent = grayStateToolTipText;
        }
        setToolTipText(toolTipContent);

        statePanel.setBackground(state.getColor());
        if (idiotLight != null) {
            idiotLight.setState(state, toolTipContent);
        }
    }

    /**
     * Sets the state of the {@link Indicator} to a specified value, and sets the tooltip text to a
     * specified {@link String}. If the state is GREEN or GRAY, and the tooltip text for these
     * states is set, then the tooltip text argument to this method will be ignored. The color and
     * tooltip text are applied to the {@link Indicator} and its {@link IdiotLight}, if any.
     */
    public void setState(State state, String toolTipText) {
        setToolTipContent(toolTipText);
        setState(state);
    }

    public State getState() {
        return state;
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new java.awt.Dimension(200, 75));
            setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
            setVerifyInputWhenFocusTarget(false);
            addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    rootMouseClicked();
                }
            });
            this.add(getTopPanel(), BorderLayout.NORTH);
            this.add(getDataPanel(), BorderLayout.CENTER);
            setComponentPopupMenu(this, getPopupMenu());
            setState(state);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getTopPanel() {
        if (topPanel == null) {
            topPanel = new JPanel();
            BorderLayout topPanelLayout = new BorderLayout();
            topPanelLayout.setHgap(5);
            topPanel.setLayout(topPanelLayout);
            topPanel.add(getNamelabel(), BorderLayout.WEST);
            topPanel.add(getStatePanel(), BorderLayout.CENTER);
        }
        return topPanel;
    }

    private JLabel getNamelabel() {
        if (namelabel == null) {
            namelabel = new JLabel();
            namelabel.setText(indicatorDisplayName);
        }
        return namelabel;
    }

    private JPanel getStatePanel() {
        if (statePanel == null) {
            statePanel = new JPanel();
            statePanel.setBackground(new java.awt.Color(0, 167, 0));
            statePanel.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
        }
        return statePanel;
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridLayout dataPanelLayout = new GridLayout(3, 2);
            dataPanelLayout.setColumns(2);
            dataPanelLayout.setRows(3);
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.setBackground(new java.awt.Color(255, 255, 255));
        }
        return dataPanel;
    }

    public static State summaryState(Indicator... indicators) {
        State sumState = State.GRAY;
        for (Indicator indicator : indicators) {
            if (indicator == null) {
                continue;
            }
            if (indicator.getState() == State.GREEN) {
                if (sumState == State.GRAY) {
                    sumState = State.GREEN;
                }
            }
            if (indicator.getState() == State.AMBER) {
                if (sumState == State.GRAY || sumState == State.GREEN) {
                    sumState = State.AMBER;
                }
            }
            if (indicator.getState() == State.RED) {
                sumState = State.RED;
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
            if (indicator.getState() == State.AMBER || indicator.getState() == State.RED) {
                stateStrings.add(indicator.getToolTipContent());
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

    public void addDataComponent(Component component) {
        dataPanel.add(component);
    }

    /**
     * @return Returns the indicatorName.
     */
    public String getIndicatorDisplayName() {
        return indicatorDisplayName;
    }

    /**
     * @param indicatorName The indicatorName to set.
     */
    public void setIndicatorDisplayName(String indicatorName) {
        indicatorDisplayName = indicatorName;
        namelabel.setText(indicatorName);
    }

    private void rootMouseClicked() {
        fireIndicatorListeners();
    }

    private void fireIndicatorListeners() {
        for (IndicatorListener listener : listeners) {
            listener.clicked(this);
        }
    }

    public boolean addIndicatorListener(IndicatorListener o) {
        return listeners.add(o);
    }

    public void clearIndicatorListeners() {
        listeners.clear();
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setLastUpdatedNow() {
        lastUpdated = System.currentTimeMillis();
    }

    private JPopupMenu getPopupMenu() {
        if (popupMenu == null) {
            popupMenu = new JPopupMenu();
            popupMenu.add(getDismissMenuItem());
        }
        return popupMenu;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });
    }

    private JMenuItem getDismissMenuItem() {
        if (dismissMenuItem == null) {
            dismissMenuItem = new JMenuItem();
            dismissMenuItem.setText("dismiss");
            dismissMenuItem.addActionListener(evt -> dismissMenuItemActionPerformed());
        }
        return dismissMenuItem;
    }

    private void dismissMenuItemActionPerformed() {
        parentIndicatorPanel.removeIndicator(this);

        // TODO add your code for dismissMenuItem.actionPerformed
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public IdiotLight getIdiotLight() {
        return idiotLight;
    }

    public void setIdiotLight(IdiotLight idiotLight) {
        this.idiotLight = idiotLight;
    }

    public String getToolTipContent() {
        return toolTipContent;
    }

    public void setToolTipContent(String toolTipText) {
        toolTipContent = toolTipText;
        setToolTipText(toolTipText);
        if (idiotLight != null) {
            idiotLight.setToolTipText(toolTipText);
        }
    }

    public String getGreenStateToolTipText() {
        return greenStateToolTipText;
    }

    public void setGreenStateToolTipText(String greenStateToolTipText) {
        this.greenStateToolTipText = greenStateToolTipText;
    }

    public String getGrayStateToolTipText() {
        return grayStateToolTipText;
    }

    public void setGrayStateToolTipText(String grayStateToolTipText) {
        this.grayStateToolTipText = grayStateToolTipText;
    }

    /**
     * Visual representation of one of the status summary panel idiot lights, in this case a filled
     * circle of one of the standard solid colors.
     *
     * @author PT
     */
    public static class IdiotLight extends JPanel {

        private static final long serialVersionUID = 1L;
        private static final int LIGHT_SIZE = 12;
        private State state = State.GREEN;

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

}
