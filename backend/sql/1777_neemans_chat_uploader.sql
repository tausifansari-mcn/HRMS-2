-- Registers the upload_template_master entry for Neemans Chat -- the new
-- upload type sql/1776's table serves. Required/optional columns are the
-- real headers confirmed directly against the file the user supplied.
-- Applied ahead of the table existing (same pattern as sql/1767/1769/
-- 1771/1773/1775) -- harmless; uploads will fail with a table-not-found
-- error until sql/1776 is run.
INSERT INTO upload_template_master
  (id, upload_type_code, upload_type_name, target_table, description, required_columns, optional_columns, sample_row, active_status)
VALUES
  (UUID(), 'NEEMANS_CHAT_MASMIS', 'Neemans — neemans_chat (writes to db_masmis.neemans_chat)', 'db_masmis.neemans_chat',
   'neemans_chat export -- writes into a new table.',
   JSON_ARRAY('Ticket ID'),
   JSON_ARRAY('Inbox ID','Inbox Name','Identifier','Ticket Status','Agent Name','Email','Phone Number','Instagram Username','Created At','Assigned At','Agent FRT At','FRT','Resolution Time At','Resolution Time','Average Wait Time','Average Handled Time','CSAT Rating','Agent Message Blocks','Is Resolved?','Is Outside Working Hours','FRT (IN Sec)','Resolution Time (In Min)','FRT TAT','Resolution TAT','Date','Time Slot','EMP ID','LOB','Week','Hour'),
   JSON_OBJECT('Ticket ID', 'SAMPLE'),
   1);
