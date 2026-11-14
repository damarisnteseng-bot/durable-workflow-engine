-- Creates 50 workflows of 5 steps each:
-- download -> validate -> (enrich, archive) -> notify
TRUNCATE jobs, job_dependencies, step_executions, effects RESTART IDENTITY;

DO $$
DECLARE wf INT; a INT; b INT; c INT; d INT; e INT;
BEGIN
  FOR wf IN 1..50 LOOP
    INSERT INTO jobs (workflow_id, step_name) VALUES (wf, 'download') RETURNING id INTO a;
    INSERT INTO jobs (workflow_id, step_name) VALUES (wf, 'validate') RETURNING id INTO b;
    INSERT INTO jobs (workflow_id, step_name) VALUES (wf, 'enrich')   RETURNING id INTO c;
    INSERT INTO jobs (workflow_id, step_name) VALUES (wf, 'archive')  RETURNING id INTO d;
    INSERT INTO jobs (workflow_id, step_name) VALUES (wf, 'notify')   RETURNING id INTO e;
    INSERT INTO job_dependencies VALUES (b, a), (c, b), (d, b), (e, c), (e, d);
  END LOOP;
END $$;