CREATE TABLE IF NOT EXISTS jars (
    jar_id UUID PRIMARY KEY,
    name text,
    upload_time timestamp,
    jar blob
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id text PRIMARY KEY,
    context_name text,
    jar_id UUID,
    classpath text,
    start_time timestamp,
    end_time timestamp,
    error text
);

CREATE TABLE IF NOT EXISTS configs (
    job_id text PRIMARY KEY,
    config text
);

