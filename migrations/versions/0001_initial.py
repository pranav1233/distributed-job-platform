"""Initial schema: api_keys, jobs, job_events, outbox, job_effects.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE api_keys (
            id uuid PRIMARY KEY,
            owner text NOT NULL,
            name text NOT NULL DEFAULT 'default',
            key_hash text NOT NULL UNIQUE,
            rate_limit_capacity integer,
            rate_limit_refill_per_sec double precision,
            created_at timestamptz NOT NULL DEFAULT now(),
            revoked_at timestamptz
        )
        """
    )
    op.execute(
        """
        CREATE TABLE jobs (
            id uuid PRIMARY KEY,
            owner text NOT NULL,
            type text NOT NULL,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            priority smallint NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
            status text NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued','running','succeeded','retry_scheduled','dead','cancelled')),
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
            version integer NOT NULL DEFAULT 1,
            available_at timestamptz NOT NULL DEFAULT now(),
            lease_owner text,
            lease_expires_at timestamptz,
            result jsonb,
            last_error text,
            idempotency_key text,
            request_fingerprint text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            CONSTRAINT uq_jobs_owner_idempotency_key UNIQUE (owner, idempotency_key),
            -- a lease exists if and only if the job is running
            CONSTRAINT ck_jobs_lease_iff_running CHECK (
                (status = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            )
        )
        """
    )
    op.execute("CREATE INDEX ix_jobs_owner_created ON jobs (owner, created_at DESC, id DESC)")
    op.execute(
        "CREATE INDEX ix_jobs_running_lease ON jobs (lease_expires_at) WHERE status = 'running'"
    )
    op.execute("CREATE INDEX ix_jobs_status ON jobs (status)")
    op.execute(
        """
        CREATE TABLE job_events (
            id bigserial PRIMARY KEY,
            job_id uuid NOT NULL REFERENCES jobs (id),
            from_status text,
            to_status text NOT NULL,
            attempt integer NOT NULL DEFAULT 0,
            actor text,
            reason text,
            detail jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_job_events_job ON job_events (job_id, id)")
    # job_events is an append-only audit log: reject UPDATE and DELETE at the database level
    op.execute(
        """
        CREATE FUNCTION job_events_append_only() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'job_events is append-only';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_job_events_append_only
        BEFORE UPDATE OR DELETE ON job_events
        FOR EACH ROW EXECUTE FUNCTION job_events_append_only()
        """
    )
    op.execute(
        """
        CREATE TABLE outbox (
            id bigserial PRIMARY KEY,
            job_id uuid NOT NULL REFERENCES jobs (id),
            kind text NOT NULL CHECK (kind IN ('dispatch','dlq')),
            payload jsonb NOT NULL,
            priority smallint NOT NULL DEFAULT 5,
            available_at timestamptz NOT NULL DEFAULT now(),
            created_at timestamptz NOT NULL DEFAULT now(),
            published_at timestamptz,
            publish_attempts integer NOT NULL DEFAULT 0,
            skipped_reason text,
            last_error text
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_outbox_pending ON outbox (available_at, id) WHERE published_at IS NULL"
    )
    op.execute(
        """
        CREATE TABLE job_effects (
            job_id uuid PRIMARY KEY,
            effect text NOT NULL,
            executions integer NOT NULL DEFAULT 1,
            first_worker text,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS job_effects")
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP TRIGGER IF EXISTS trg_job_events_append_only ON job_events")
    op.execute("DROP TABLE IF EXISTS job_events")
    op.execute("DROP FUNCTION IF EXISTS job_events_append_only()")
    op.execute("DROP TABLE IF EXISTS jobs")
    op.execute("DROP TABLE IF EXISTS api_keys")
