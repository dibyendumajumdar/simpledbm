package org.simpledbm.samples.forum.client;

import org.simpledbm.samples.forum.client.RequestProcessor.TopicsViewHandler;

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
        TopicsViewHandler {

    static final int VISIBLE_TOPICS_COUNT = 10;

    private int startIndex, selectedRow = -1;
    private FlexTable table = new FlexTable();

    private Topic[] topicList;

    private DockLayoutPanel panel = new DockLayoutPanel(Unit.EM);
    private FlexTable header = new FlexTable();

    private TopicsMenu navBar;

    private TopicsHandler topicsHandler;

    public TopicsViewImpl() {
        // Setup the table.
        ScrollPanel sp = new ScrollPanel();
        sp.add(table);

        panel.setStyleName("forumTopics");
        panel.addNorth(header, 2);
        panel.add(sp);
        initWidget(panel);

        navBar = new TopicsMenu(this);
        initTable();
        //        update();
    }

    @Override
    protected void onLoad() {
        // Select the first row if none is selected.
        //        if (selectedRow == -1) {
        //            selectRow(0);
        //        }
    }

    void newer() {
        // Move back a page.
        startIndex -= VISIBLE_TOPICS_COUNT;
        if (startIndex < 0) {
            startIndex = 0;
        } else {
            styleRow(selectedRow, false);
            selectedRow = -1;
            update();
        }
    }

    void older() {
        // Move forward a page.
        startIndex += VISIBLE_TOPICS_COUNT;
        if (startIndex >= topicList.length) {
            startIndex -= VISIBLE_TOPICS_COUNT;
        } else {
            styleRow(selectedRow, false);
            selectedRow = -1;
            update();
        }
    }

    void onTableClicked(ClickEvent event) {
        // Select the row that was clicked (-1 to account for header row).
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
        header.setText(0, 1, "Last Updated");
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
        // When a row (other than the first one, which is used as a header) is
        // selected, display its associated MailItem.
        Topic item = topicList[startIndex + row];
        if (item == null) {
            return;
        }

        styleRow(selectedRow, false);
        styleRow(row, true);

        item.read = true;
        selectedRow = row;

        topicsHandler.onTopicSelection(item.getTitle());
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
        int count = topicList.length;
        int max = startIndex + VISIBLE_TOPICS_COUNT;
        if (max > count) {
            max = count;
        }

        // Update the nav bar.
        navBar.update(startIndex, count, max);

        // Show the selected emails.
        int i = 0;
        for (; i < VISIBLE_TOPICS_COUNT; ++i) {
            // Don't read past the end.
            if (startIndex + i >= topicList.length) {
                break;
            }

            Topic item = topicList[startIndex + i];
            System.err.println("Adding " + item.getStartedBy());

            // Add a new row to the table, then set each of its columns to the
            // email's sender and subject values.
            table.setText(i, 0, item.getLastPoster());
            table.setText(i, 1, item.getLastPost());
            table.setText(i, 2, item.getTitle());
        }

        // Clear any remaining slots.
        //        for (; i < VISIBLE_TOPICS_COUNT; ++i) {
        //            table.removeRow(table.getRowCount() - 1);
        //        }
        System.err.println(table.getRowCount());
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
        this.topicList = topicList;
        update();
        if (selectedRow == -1) {
            selectRow(0);
        }
    }

    public void setTopicsHandler(TopicsHandler topicsHandler) {
        this.topicsHandler = topicsHandler;
    }

}
