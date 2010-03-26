package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.HTMLTable.Cell;

public class ForumTopics extends Composite implements ClickHandler {

    private static final int VISIBLE_TOPICS_COUNT = 10;

    private HTML countLabel = new HTML();
    private HTML newerButton = new HTML("<a href='javascript:;'>&lt; newer</a>",
        true);
    private HTML olderButton = new HTML("<a href='javascript:;'>older &gt;</a>",
        true);
    private int startIndex, selectedRow = -1;
    private FlexTable table = new FlexTable();
    private HorizontalPanel navBar = new HorizontalPanel();
    
    private TopicList topicList = new TopicList();

    public ForumTopics() {
      // Setup the table.
      table.setCellSpacing(0);
      table.setCellPadding(0);
      table.setWidth("100%");

      // Hook up events.
      table.addClickHandler(this);
      newerButton.addClickHandler(this);
      olderButton.addClickHandler(this);

      // Create the 'navigation' bar at the upper-right.
      HorizontalPanel innerNavBar = new HorizontalPanel();
      navBar.setStyleName("mail-ListNavBar");
      innerNavBar.add(newerButton);
      innerNavBar.add(countLabel);
      innerNavBar.add(olderButton);

      navBar.setHorizontalAlignment(HorizontalPanel.ALIGN_RIGHT);
      navBar.add(innerNavBar);
      navBar.setWidth("100%");

      initWidget(table);
      setStyleName("mail-List");

      initTable();
      update();
    }

    public void onClick(ClickEvent event) {
      Object sender = event.getSource();
      if (sender == olderButton) {
        // Move forward a page.
        startIndex += VISIBLE_TOPICS_COUNT;
        if (startIndex >= topicList.getTopicCount()) {
          startIndex -= VISIBLE_TOPICS_COUNT;
        } else {
          styleRow(selectedRow, false);
          selectedRow = -1;
          update();
        }
      } else if (sender == newerButton) {
        // Move back a page.
        startIndex -= VISIBLE_TOPICS_COUNT;
        if (startIndex < 0) {
          startIndex = 0;
        } else {
          styleRow(selectedRow, false);
          selectedRow = -1;
          update();
        }
      } else if (sender == table) {
        // Select the row that was clicked (-1 to account for header row).
        Cell cell = table.getCellForEvent(event);
        if (cell != null) {
          int row = cell.getRowIndex();
          if (row > 0) {
            selectRow(row - 1);
          }
        }
      }
    }

    /**
     * Initializes the table so that it contains enough rows for a full page of
     * emails. Also creates the images that will be used as 'read' flags.
     */
    private void initTable() {
      // Create the header row.
      table.setText(0, 0, "Topic");
      table.setText(0, 1, "Author");
      table.setText(0, 2, "Updated");
      table.setText(0, 3, "Posts");
      table.setWidget(0, 3, navBar);
      table.getRowFormatter().setStyleName(0, "mail-ListHeader");

      // Initialize the rest of the rows.
      for (int i = 0; i < VISIBLE_TOPICS_COUNT; ++i) {
        table.setText(i + 1, 0, "");
        table.setText(i + 1, 1, "");
        table.setText(i + 1, 2, "");
        table.setText(i + 1, 3, "");
        table.getCellFormatter().setWordWrap(i + 1, 0, false);
        table.getCellFormatter().setWordWrap(i + 1, 1, false);
        table.getCellFormatter().setWordWrap(i + 1, 2, false);
        table.getCellFormatter().setWordWrap(i + 1, 3, false);
        table.getFlexCellFormatter().setColSpan(i + 1, 2, 2);
      }
    }

    /**
     * Selects the given row (relative to the current page).
     * 
     * @param row the row to be selected
     */
    private void selectRow(int row) {
      // When a row (other than the first one, which is used as a header) is
      // selected, display its associated MailItem.
//      MailItem item = MailItems.getMailItem(startIndex + row);
//      if (item == null) {
//        return;
//      }
//
//      styleRow(selectedRow, false);
//      styleRow(row, true);
//
//      item.read = true;
//      selectedRow = row;
//      Mail.get().displayItem(item);
    }

    private void styleRow(int row, boolean selected) {
      if (row != -1) {
        if (selected) {
          table.getRowFormatter().addStyleName(row + 1, "mail-SelectedRow");
        } else {
          table.getRowFormatter().removeStyleName(row + 1, "mail-SelectedRow");
        }
      }
    }

    private void update() {
      // Update the older/newer buttons & label.
      int count = topicList.getTopicCount();
      int max = startIndex + VISIBLE_TOPICS_COUNT;
      if (max > count) {
        max = count;
      }

      newerButton.setVisible(startIndex != 0);
      olderButton.setVisible(startIndex + VISIBLE_TOPICS_COUNT < count);
      countLabel.setText("" + (startIndex + 1) + " - " + max + " of " + count);

      // Show the selected emails.
      int i = 0;
      for (; i < VISIBLE_TOPICS_COUNT; ++i) {
        // Don't read past the end.
        if (startIndex + i >= topicList.getTopicCount()) {
          break;
        }

        Topic item = topicList.getTopic(startIndex + i);

        // Add a new row to the table, then set each of its columns to the
        // email's sender and subject values.
        table.setText(i + 1, 0, item.getTitle());
        table.setText(i + 1, 1, item.getStartedBy());
        table.setText(i + 1, 2, item.getLastPost());
        table.setText(i + 1, 3, item.getNumPosts());
      }

      // Clear any remaining slots.
      for (; i < VISIBLE_TOPICS_COUNT; ++i) {
        table.setHTML(i + 1, 0, "&nbsp;");
        table.setHTML(i + 1, 1, "&nbsp;");
        table.setHTML(i + 1, 2, "&nbsp;");
        table.setHTML(i + 1, 3, "&nbsp;");
        
      }

      // Select the first row if none is selected.
      if (selectedRow == -1) {
        selectRow(0);
      }
    }
    
    
}
