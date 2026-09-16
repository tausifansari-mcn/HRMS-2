-- Creates 1 brand-new db_masmis table for a new Neemans uploader: Neemans
-- Chat (neemans_chat), per explicit user request. Columns confirmed
-- directly against the real file the user supplied
-- (C:\Users\MAS60358\Desktop\Clovia\Neamans Chat.xlsx) -- not guessed.
-- A Kwikengage-style chat/DM ticket export (Neeman's WA + Instagram DM
-- inboxes), same shape as GNC Chat (sql/1774). Checked information_schema
-- -- no existing neemans_chat or equivalent table exists.
--
-- This app's db_masmis user has no CREATE privilege (confirmed live
-- repeatedly this session) -- NOT applied by this session; the user is
-- running it themselves.

CREATE TABLE IF NOT EXISTS db_masmis.neemans_chat (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ticket_id VARCHAR(50) NULL,
  inbox_id VARCHAR(50) NULL,
  inbox_name VARCHAR(150) NULL,
  identifier VARCHAR(200) NULL,
  ticket_status VARCHAR(50) NULL,
  agent_name VARCHAR(150) NULL,
  email VARCHAR(150) NULL,
  phone_number VARCHAR(50) NULL,
  instagram_username VARCHAR(150) NULL,
  created_at_src VARCHAR(50) NULL,
  assigned_at VARCHAR(50) NULL,
  agent_frt_at VARCHAR(50) NULL,
  frt VARCHAR(20) NULL,
  resolution_time_at VARCHAR(50) NULL,
  resolution_time VARCHAR(20) NULL,
  avg_wait_time VARCHAR(20) NULL,
  avg_handled_time VARCHAR(20) NULL,
  csat_rating VARCHAR(20) NULL,
  agent_message_blocks VARCHAR(20) NULL,
  is_resolved VARCHAR(10) NULL,
  is_outside_working_hours VARCHAR(10) NULL,
  frt_in_sec VARCHAR(20) NULL,
  resolution_time_in_min VARCHAR(20) NULL,
  frt_tat VARCHAR(20) NULL,
  resolution_tat VARCHAR(20) NULL,
  report_date VARCHAR(50) NULL,
  time_slot VARCHAR(20) NULL,
  emp_id VARCHAR(50) NULL,
  lob VARCHAR(50) NULL,
  week VARCHAR(20) NULL,
  hour_val VARCHAR(20) NULL,
  uploaded_by INT NULL,
  upload_batch_id VARCHAR(36) NULL,
  inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_neemans_chat_batch (upload_batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
