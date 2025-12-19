import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import dacite

import dateparser
import yaml
from psycopg import Connection

from periodic_database_jobs import periodic_db_logger

@dataclass
class Job:
    name: str
    schedule: str
    sql: str
    transaction: bool


def _load_state(path: Path) -> dict[str, dt.datetime]:
    if not path.exists():
        periodic_db_logger.debug("State file does not exist: %s", path)
        return {}

    data = yaml.safe_load(path.read_text()) or {}
    state: dict[str, dt.datetime] = {}

    for job, ts in data.items():
        parsed = dateparser.parse(ts) if ts else None
        if parsed:
            state[job] = parsed
        else:
            periodic_db_logger.warning("Invalid timestamp for job '%s' in %s", job, path)

    periodic_db_logger.debug("Loaded state from %s: %s", path, state)
    return state


def _save_state(path: Path, state: dict[str, dt.datetime]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialised = {k: v.isoformat() for k, v in state.items()}
    path.write_text(yaml.safe_dump(serialised, sort_keys=True))
    periodic_db_logger.info("Updated state file %s", path)


def _should_run(*, schedule_spec: str, last_run: dt.datetime | None, now: dt.datetime) -> bool:
    next_time = dateparser.parse(
        schedule_spec,
        settings={
            "RELATIVE_BASE": last_run or now,
            "PREFER_DATES_FROM": "future",
        },
    )

#    next_time = dateparser.parse(schedule_spec)
    if next_time is None:
        raise ValueError(f"Could not parse schedule: {schedule_spec}")

    if last_run is None:
        periodic_db_logger.debug( "schedule='%s', no last_run", schedule_spec)
        return True

    decision = last_run < next_time <= now
    periodic_db_logger.debug(
        "schedule='%s', last_run=%s, next_time=%s, now=%s, should_run=%s",
        schedule_spec, last_run, next_time, now, decision,
    )
    return decision


def run_jobs(config, conn: Connection[Any]) -> None:
    """
    Read jobs from YAML, evaluate schedules, run due jobs via psycopg3,
    and update a single shared state file.
    """
    now = dt.datetime.now()

    periodic_db_logger.info("Starting job evaluation at %s", now.isoformat())

    state_file = config["state_file"]
    jobs = config.get("jobs", {})


    if not jobs:
        periodic_db_logger.warning("No jobs defined")
        return

    state_path = Path(state_file)
    state = _load_state(state_path)

    for j in jobs:
        job = dacite.from_dict(Job,j)
        periodic_db_logger.debug("Evaluating job '%s'", job.name)

        last_run = state.get(job.name)

        if not _should_run( schedule_spec=job.schedule, last_run=last_run, now=now, ):
            periodic_db_logger.debug("Job '%s' is not due", job.name)
            continue

        periodic_db_logger.info("Running job '%s'", job.name)
        try:
            conn.autocommit = not job.transaction
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(job.sql)
            conn.commit()
        except Exception:
            conn.rollback()
            periodic_db_logger.exception("Job '%s' failed; transaction rolled back", job.name)
            raise

        state[job.name] = now
        _save_state(state_path, state)
        periodic_db_logger.info("Job '%s' completed successfully", job.name)
