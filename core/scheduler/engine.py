"""Config-driven cron scheduler for builds + EOD snapshots.

Reads ``config/schedule.yaml`` (or ``$SCHEDULE_CONFIG_PATH``) and
schedules two job kinds:

- **builds** — call ``IncrementalGraphBuilder.run_build()`` on cron;
  each fire is numbered sequentially within the same calendar date so
  ``graph_build_runs`` records (build_date, build_number) cleanly.
- **snapshots** — call ``SnapshotScheduler.take_daily_snapshot()`` once
  per day at the configured time (typically just after the last build).

The engine owns its own polling loop (``run_loop``) — registered via
``asyncio.create_task`` from the API lifespan. It is a NO-OP unless
the env var ``ENABLE_SCHEDULE_ENGINE=true`` is set, so unit tests +
local dev never accidentally fire scheduled builds.

Cron evaluation uses ``croniter`` with the YAML's ``timezone`` (default
``US/Eastern``). The "due" check compares the cron's most recent fire
against the engine's recorded ``last_runs[name]`` — so a 5-min poll
catches every cron tick exactly once even if the loop sleeps slightly
past the boundary.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Optional

import yaml


# Match shell-style ``${NAME}`` and ``${NAME:-default}`` references so a
# single YAML can carry sensible local-fs defaults while ECS task
# definitions inject the real S3 / API URL via env vars. Keeps the
# config file checked in + tenant-agnostic; the deploy substitutes
# the runtime values without sed/jinja gymnastics.
_ENV_VAR_RE = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")


def _resolve_env_vars(value: Any) -> Any:
    """Recursively walk a parsed-YAML structure and substitute every
    ``${VAR}`` / ``${VAR:-default}`` reference inside string leaves.
    Non-string scalars (ints, bools, None) are passed through unchanged."""
    if isinstance(value, str):
        return _ENV_VAR_RE.sub(
            lambda m: os.getenv(m.group(1), m.group(2) or ""),
            value,
        )
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    return value

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timezone helpers — prefer stdlib zoneinfo (Python 3.9+); fall back to
# pytz only when zoneinfo is unavailable.
# ---------------------------------------------------------------------------


def _resolve_tz(name: str):
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:
        try:
            import pytz
            return pytz.timezone(name)
        except Exception:
            return timezone.utc


# ---------------------------------------------------------------------------
# ScheduleEngine
# ---------------------------------------------------------------------------


class ScheduleEngine:
    """Owner of the YAML config + the cron loop.

    The engine is constructed once per API process. It expects the
    builder + snapshot_scheduler + connector to be wired up by the
    caller (the lifespan handler in api/main.py); it never builds
    them itself so swapping in test fakes stays trivial.
    """

    def __init__(
        self,
        config_path: str = "config/schedule.yaml",
        builder=None,
        snapshot_scheduler=None,
        connector=None,
    ):
        self.config_path       = config_path
        self.config            = self._load_config(config_path)
        self.builder           = builder
        self.snapshot_scheduler = snapshot_scheduler
        self.connector         = connector
        self.last_runs: dict[str, datetime] = {}
        self._tz_name = self.config.get("schedule", {}).get(
            "timezone", "US/Eastern",
        )
        self.tz = _resolve_tz(self._tz_name)
        self._stop = asyncio.Event()
        # Track per-day build-number sequence so graph_build_runs rows
        # carry stable (build_date, build_number) keys without depending
        # on a DB count query.
        self._build_seq: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Config IO
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(path: str) -> dict:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return _resolve_env_vars(raw)

    def reload(self) -> dict:
        """Re-read the YAML in place + refresh dependent fields. Returns
        the new config dict so callers can echo it back to operators."""
        self.config = self._load_config(self.config_path)
        self._tz_name = self.config.get("schedule", {}).get(
            "timezone", "US/Eastern",
        )
        self.tz = _resolve_tz(self._tz_name)
        return self.config

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        env_override = os.getenv("SCHEDULE_ENABLED")
        if env_override is not None:
            return env_override.lower() == "true"
        return bool(self.config.get("schedule", {}).get("enabled", True))

    def is_snapshot_enabled(self) -> bool:
        return bool(self.config.get("schedule", {}).get("snapshot_enabled", True))

    def status(self) -> dict:
        """Snapshot of current config + last_runs + next-fire times.
        Powers ``GET /scheduler/status``."""
        from croniter import croniter

        now = datetime.now(self.tz)
        sched = self.config.get("schedule", {}) or {}

        def _job_status(jobs: list, kind: str) -> list:
            out: list[dict] = []
            for j in jobs or []:
                cron_expr = j.get("cron", "")
                next_fire = None
                last_fire = None
                try:
                    next_fire = croniter(cron_expr, now).get_next(datetime)
                    last_fire = croniter(cron_expr, now).get_prev(datetime)
                except Exception:
                    pass
                last_run = self.last_runs.get(j.get("name"))
                out.append({
                    "name":        j.get("name"),
                    "kind":        kind,
                    "cron":        cron_expr,
                    "description": j.get("description"),
                    "last_run":    last_run.isoformat() if last_run else None,
                    "next_fire":   next_fire.isoformat() if next_fire else None,
                    "last_fire":   last_fire.isoformat() if last_fire else None,
                })
            return out

        return {
            "enabled":           self.is_enabled(),
            "snapshot_enabled":  self.is_snapshot_enabled(),
            "timezone":          self._tz_name,
            "poll_interval":     int(sched.get("poll_interval", 300)),
            "now":               now.isoformat(),
            "builds":            _job_status(sched.get("builds", []), "build"),
            "snapshots":         _job_status(sched.get("snapshots", []), "snapshot"),
            "connector":         sched.get("connector", {}) or {},
            "builder":           sched.get("builder", {}) or {},
            "config_path":       self.config_path,
        }

    # ------------------------------------------------------------------
    # Cron evaluation
    # ------------------------------------------------------------------

    def _is_due(self, name: str, cron_expr: str, now: datetime) -> bool:
        """Cron fire was within the last ``poll_interval`` seconds AND
        we haven't recorded a run for it yet."""
        try:
            from croniter import croniter
            cron = croniter(cron_expr, now)
            prev_fire = cron.get_prev(datetime)
        except Exception as exc:
            logger.warning(
                "scheduler_cron_parse_failed",
                extra={"job_name": name, "cron": cron_expr, "error": str(exc)[:200]},
            )
            return False

        last_run = self.last_runs.get(name)
        if last_run and last_run >= prev_fire:
            return False

        poll = int(self.config.get("schedule", {}).get("poll_interval", 300))
        seconds_since_fire = (now - prev_fire).total_seconds()
        return 0 <= seconds_since_fire < poll

    def get_due_jobs(self, now: Optional[datetime] = None) -> list[dict]:
        if now is None:
            now = datetime.now(self.tz)
        sched = self.config.get("schedule", {}) or {}
        due: list[dict] = []
        for build in sched.get("builds", []) or []:
            if self._is_due(build["name"], build["cron"], now):
                due.append({"type": "build", **build})
        if self.is_snapshot_enabled():
            for snap in sched.get("snapshots", []) or []:
                if self._is_due(snap["name"], snap["cron"], now):
                    due.append({"type": "snapshot", **snap})
        return due

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _next_build_number(self, day_key: str) -> int:
        """Sequential per-day build number for graph_build_runs."""
        n = self._build_seq.get(day_key, 0) + 1
        self._build_seq[day_key] = n
        return n

    @property
    def _tenant_id(self) -> str:
        return (self.config.get("schedule", {})
                           .get("builder", {})
                           .get("tenant_id", "default"))

    async def run_build_job(self, job: dict, now: datetime) -> dict:
        """Execute a single build job and return its stats. Stamps
        ``last_runs[name]`` on success or failure (so a flapping job
        doesn't stampede the next poll)."""
        name = job.get("name", "build")
        if not self.builder:
            logger.warning("scheduler_build_no_builder", extra={"job_name": name})
            self.last_runs[name] = now
            return {"error": "no_builder_configured"}

        day_key      = now.date().isoformat()
        build_number = self._next_build_number(day_key)
        try:
            stats = await self.builder.run_build(
                build_date=now.date(),
                build_number=build_number,
                tenant_id=self._tenant_id,
            )
            logger.info(
                "scheduler_build_complete",
                extra={"job_name": name, "build_number": build_number, **(stats or {})},
            )
            return {"name": name, "build_number": build_number, **(stats or {})}
        except Exception as exc:
            logger.error(
                "scheduler_build_failed",
                extra={"job_name": name, "error": str(exc)[:200]},
            )
            return {"name": name, "error": str(exc)[:500]}
        finally:
            self.last_runs[name] = now

    async def run_snapshot_job(self, job: dict, now: datetime) -> dict:
        name = job.get("name", "snapshot")
        if not self.snapshot_scheduler:
            logger.warning("scheduler_snapshot_no_scheduler", extra={"job_name": name})
            self.last_runs[name] = now
            return {"error": "no_snapshot_scheduler_configured"}
        try:
            count = await self.snapshot_scheduler.take_daily_snapshot(
                snapshot_date=now.date(),
                tenant_id=self._tenant_id,
            )
            logger.info(
                "scheduler_snapshot_complete",
                extra={"job_name": name, "entities": count},
            )
            return {"name": name, "entities": count}
        except Exception as exc:
            logger.error(
                "scheduler_snapshot_failed",
                extra={"job_name": name, "error": str(exc)[:200]},
            )
            return {"name": name, "error": str(exc)[:500]}
        finally:
            self.last_runs[name] = now

    async def run_due_jobs(self) -> list[dict]:
        now = datetime.now(self.tz)
        due = self.get_due_jobs(now)
        results: list[dict] = []
        for job in due:
            if job["type"] == "build":
                results.append(await self.run_build_job(job, now))
            else:
                results.append(await self.run_snapshot_job(job, now))
        return results

    async def trigger_job(self, job_name: str) -> dict:
        """Manual trigger — bypasses the cron-due check. Used by
        ``POST /scheduler/trigger`` for testing + ad-hoc backfills."""
        sched = self.config.get("schedule", {}) or {}
        for j in sched.get("builds", []) or []:
            if j.get("name") == job_name:
                return await self.run_build_job(j, datetime.now(self.tz))
        for j in sched.get("snapshots", []) or []:
            if j.get("name") == job_name:
                return await self.run_snapshot_job(j, datetime.now(self.tz))
        return {"error": f"unknown job: {job_name}"}

    async def run_catch_up(
        self, max_builds: int = 200,
    ) -> dict:
        """Drain a backlog of un-pulled S3 docs synchronously.

        Walks the connector forward in 500-doc chunks until a build
        comes back with ``documents_new == 0`` (no more new data).
        When the watermark crosses a calendar-day boundary, takes an
        EOD snapshot for the prior day so the lineage view stays
        accurate. Returns aggregate stats for the operator.

        ``max_builds`` is a safety cap so a runaway loop can't lock
        the API forever — at 500 docs/build × 200 builds = 100k docs
        per call, well above the 9k-loan / ~270k-doc scale target.
        """
        sched = self.config.get("schedule", {}) or {}
        build_cfg = next(iter(sched.get("builds", []) or [{}]), {}) or {}
        builds = 0
        total_docs_new = 0
        snapshots_taken = 0
        last_wm_date = None
        while builds < max_builds:
            now = datetime.now(self.tz)
            stats = await self.run_build_job(build_cfg, now)
            builds += 1
            total_docs_new += int(stats.get("documents_new") or 0)
            wm_to = stats.get("watermark_to") or stats.get("watermark_from")
            sim_date = None
            if wm_to:
                try:
                    sim_date = (
                        wm_to.split("T")[0]
                        if isinstance(wm_to, str)
                        else wm_to.date().isoformat()
                    )
                except Exception:
                    sim_date = None
            # Snapshot when the simulation watermark crosses a day.
            if (sim_date and last_wm_date
                    and sim_date != last_wm_date
                    and self.snapshot_scheduler):
                try:
                    n = await self.snapshot_scheduler.take_daily_snapshot(
                        snapshot_date=date.fromisoformat(last_wm_date),
                        tenant_id=self._tenant_id,
                    )
                    snapshots_taken += 1
                    logger.info(
                        f"catch_up_snapshot date={last_wm_date} entities={n}"
                    )
                except Exception as exc:
                    logger.warning(
                        f"catch_up_snapshot_failed date={last_wm_date} "
                        f"error={str(exc)[:200]}"
                    )
            last_wm_date = sim_date or last_wm_date

            new_docs = int(stats.get("documents_new") or 0)
            new_apps = int(stats.get("applications_created") or 0)
            pulled   = int(stats.get("documents_pulled") or 0)
            logger.info(
                f"catch_up_build build={builds} docs_pulled={pulled} "
                f"docs_new={new_docs} apps_new={new_apps} "
                f"total_new={total_docs_new} sim_date={sim_date}"
            )
            # Stop only when the connector found ABSOLUTELY NOTHING in
            # the next date folder — i.e., we've reached the end of
            # the corpus. ``documents_new == 0`` alone means "this
            # day's docs were all dedup-skipped because they're
            # already in PG", which can happen when re-running catch-
            # up over partially-indexed dates; the next folder might
            # still have brand-new content. Only ``documents_pulled
            # == 0 && applications_created == 0`` means truly idle.
            if pulled == 0 and new_apps == 0:
                # Final EOD snapshot for the last day we processed.
                if last_wm_date and self.snapshot_scheduler:
                    try:
                        n = await self.snapshot_scheduler.take_daily_snapshot(
                            snapshot_date=date.fromisoformat(last_wm_date),
                            tenant_id=self._tenant_id,
                        )
                        snapshots_taken += 1
                    except Exception:
                        pass
                break
        return {
            "builds":              builds,
            "documents_processed": total_docs_new,
            "snapshots_taken":     snapshots_taken,
            "stopped_reason":      ("max_builds_hit" if builds >= max_builds
                                     else "no_new_docs"),
        }

    # ------------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------------

    async def run_loop(self) -> None:
        """Poll forever, checking + running due jobs on each tick."""
        poll = int(self.config.get("schedule", {}).get("poll_interval", 300))
        sched = self.config.get("schedule", {}) or {}
        logger.info(
            "scheduler_started",
            extra={
                "poll_interval": poll,
                "timezone":      self._tz_name,
                "builds":        len(sched.get("builds", []) or []),
                "snapshots":     len(sched.get("snapshots", []) or []),
            },
        )
        try:
            while not self._stop.is_set():
                if self.is_enabled():
                    try:
                        await self.run_due_jobs()
                    except Exception as exc:
                        logger.error(
                            "scheduler_tick_unhandled",
                            extra={"error": str(exc)[:200]},
                        )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=poll)
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise
        finally:
            logger.info("scheduler_stopped")

    def stop(self) -> None:
        self._stop.set()
