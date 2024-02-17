CREATE SCHEMA IF NOT EXISTS datalake;

CREATE TABLE IF NOT EXISTS datalake.user_signed_in_at (
  id SERIAL PRIMARY KEY,
  user_id INT,
  date INT,
  month INT,
  year INT,
  created_time BIGINT
);

CREATE TABLE IF NOT EXISTS datalake.user_signed_up_at (
  id SERIAL PRIMARY KEY,
  user_id INT,
  date INT,
  month INT,
  year INT,
  created_time BIGINT
);