-- Runs once on first Postgres start: a separate role + database for the benchmark source.
CREATE ROLE bench LOGIN PASSWORD 'bench';
CREATE DATABASE source OWNER bench;
\connect source
CREATE SCHEMA bench AUTHORIZATION bench;
