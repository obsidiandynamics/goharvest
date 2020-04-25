#!/bin/sh

cat <<EOF | psql -U postgres -h localhost
CREATE TABLE IF NOT EXISTS outbox (
  id                  BIGSERIAL PRIMARY KEY,
  create_time         TIMESTAMP WITH TIME ZONE NOT NULL,
  kafka_topic         VARCHAR(249) NOT NULL,
  kafka_key           VARCHAR(100) NOT NULL,
  kafka_value         VARCHAR(10000),
  kafka_header_keys   TEXT[] NOT NULL,
  kafka_header_values TEXT[] NOT NULL,
  leader_id           UUID
)
EOF
