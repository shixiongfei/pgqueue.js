-- db.sql
--
-- Copyright (c) 2023-2024 Xiongfei Shi
--
-- Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
-- License: Apache-2.0
--
-- https://github.com/shixiongfei/pgqueue.js
--

-- Message queue base table
DROP TABLE IF EXISTS pgqueue_table;
CREATE TABLE pgqueue_table
(
    id character varying(26),
    payload jsonb,
    visible_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
CREATE INDEX ix_pgqueue_table_visible_at
    ON pgqueue_table USING btree
    (visible_at, created_at ASC NULLS LAST)
;
