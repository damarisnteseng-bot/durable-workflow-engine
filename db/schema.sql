CREATE SEQUENCE workflow_id_seq START 1000;

CREATE TABLE jobs (
  id               SERIAL PRIMARY KEY,
  status           TEXT NOT NULL DEFAULT 'PENDING',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by       TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempts         INT NOT NULL DEFAULT 0,
  max_attempts     INT NOT NULL DEFAULT 3,
  run_after        TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error       TEXT,
  workflow_id      INT,
  workflow_name    TEXT,
  step_name        TEXT,
  finished_at      TIMESTAMPTZ
);

CREATE TABLE job_dependencies (
  job_id        INT NOT NULL REFERENCES jobs(id),
  depends_on_id INT NOT NULL REFERENCES jobs(id),
  PRIMARY KEY (job_id, depends_on_id)
);

CREATE TABLE step_executions (
  id         BIGSERIAL PRIMARY KEY,
  job_id     INT NOT NULL,
  worker     TEXT,
  attempt    INT,
  started_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE effects (
  idempotency_key TEXT PRIMARY KEY,
  job_id          INT NOT NULL,
  step_name       TEXT,
  worker          TEXT,
  times_requested INT NOT NULL DEFAULT 1,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);