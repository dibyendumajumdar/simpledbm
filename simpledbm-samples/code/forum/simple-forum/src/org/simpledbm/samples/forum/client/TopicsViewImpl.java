package org.simpledbm.samples.forum.client;

import org.simpledbm.samples.forum.client.RequestProcessor.TopicsView;

import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.ResizeComposite;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.HTMLTable.Cell;

public class TopicsViewImpl extends ResizeComposite implements ClickHandler,
        TopicsView {

    static final int VISIBLE_TOPICS_COUNT = 10;

    /**
     * Points to the row that is at the top of the page. Ranges from 0 to
     * VISIBLE_TOPICS_COUNT-1.
     */
    private int startIndex;

    /**
     * Points to the topic that is selected. Ranges from 0 to topics.length-1.
     */
    private int selectedRow = -1;
    private FlexTable table = new FlexTable();

    private Topic[] topics;

    private DockLayoutPanel panel = new DockLayoutPanel(Unit.EM);
    private FlexTable header = new FlexTable();

    private TopicsMenu navBar;

    private TopicsHandler topicsHandler;

    public TopicsViewImpl() {
        ScrollPanel sp = new ScrollPanel();
        sp.add(table);
        panel.setStyleName("forumTopics");
        panel.addNorth(header, 2);
        panel.add(sp);
        initWidget(panel);
        navBar = new TopicsMenu(this);
        initTable();
    }

    void newer() {
        // Move back a page.
        if (startIndex == 0) {
            return;
        }
        startIndex -= VISIBLE_TOPICS_COUNT;
        if (startIndex < 0) {
            startIndex = 0;
        }
        update();
        selectRow(selectedRow);
    }

    void older() {
        // Move forward a page.
        startIndex += VISIBLE_TOPICS_COUNT;
        if (startIndex >= topics.length) {
            startIndex = Math.max(topics.length - VISIBLE_TOPICS_COUNT, 0);
        }
        update();
        selectRow(selectedRow);
    }

    void onTableClicked(ClickEvent event) {
        Cell cell = table.getCellForEvent(event);
        if (cell != null) {
            int row = cell.getRowIndex();
            selectRow(row);
        }
    }

    /**
     * Initializes the table so that it contains enough rows for a full page of
     * emails. Also creates the images that will be used as 'read' flags.
     */
    private void initTable() {
        // Initialize the header.
        header.setCellPadding(0);
        header.setCellSpacing(0);
        header.setStyleName("header");
        header.getColumnFormatter().setWidth(0, "128px");
        header.getColumnFormatter().setWidth(1, "192px");
        header.getColumnFormatter().setWidth(3, "256px");

        header.setText(0, 0, "Author");
        header.setText(0, 1, "Num Posts");
        header.setText(0, 2, "Title");
        header.setWidget(0, 3, navBar);
        header.getCellFormatter().setHorizontalAlignment(0, 3,
                HasHorizontalAlignment.ALIGN_RIGHT);

        // Initialize the table.
        table.setCellPadding(0);
        table.setCellSpacing(0);
        table.setStyleName("table");
        table.getColumnFormatter().setWidth(0, "128px");
        table.getColumnFormatter().setWidth(1, "192px");

        table.addClickHandler(this);
    }

    /**
     * Selects the given row (relative to the current page).
     * 
     * @param row the row to be selected
     */
    private void selectRow(int row) {
        if ((startIndex + row) > topics.length) {
            row = topics.length - startIndex - 1;
        }
        int pos = startIndex + row;
        if (pos >= topics.length) {
            System.err.println("WARNING: pos >= topics.length");
            return;
        }
        Topic item = topics[pos];
        styleRow(selectedRow, false);
        styleRow(row, true);
        selectedRow = row;
        topicsHandler.onTopicSelection(item);
    }

    private void styleRow(int row, boolean selected) {
        if (row != -1) {
            if (selected) {
                table.getRowFormatter().addStyleName(row, "selectedRow");
            } else {
                table.getRowFormatter().removeStyleName(row, "selectedRow");
            }
        }
    }

    private void update() {
        // Update the older/newer buttons & label.
        int count = topics.length;
        int max = startIndex + VISIBLE_TOPICS_COUNT;
        if (max > count) {
            max = count;
        }

        // Update the nav bar.
        navBar.update(startIndex, count, max);

        // Show the topics.
        table.removeAllRows();
        int i = 0;
        for (; i < VISIBLE_TOPICS_COUNT; ++i) {
            // Don't read past the end.
            if (startIndex + i >= topics.length) {
                break;
            }

            Topic item = topics[startIndex + i];
            table.setText(i, 0, item.getLastPoster());
            table.setText(i, 1, Integer.toString(item.getNumPosts()));
            table.setText(i, 2, item.getTitle());
        }
    }

    public void onClick(ClickEvent event) {
        Object sender = event.getSource();
        if (sender == table) {
            // Select the row that was clicked (-1 to account for header row).
            onTableClicked(event);
        }
    }

    public void showBusy() {
    }

    public void update(Topic[] topicList) {
        this.topics = topicList;
        update();
        if (selectedRow == -1) {
            selectRow(0);
        }
    }

    public void setTopicsHandler(TopicsHandler topicsHandler) {
        this.topicsHandler = topicsHandler;
        this.navBar.setTopicsHandler(topicsHandler);
    }

}
