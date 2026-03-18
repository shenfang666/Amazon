from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

ROOT: Path | None = None
ETL_RUNNER: Path | None = None
NOW_ISO = None

MONTHLY_JOB: dict = {
    "job_id": None,
    "status": "idle",
    "target_month": None,
    "skip_init": True,
    "started_at": None,
    "finished_at": None,
    "exit_code": None,
    "logs": [],
}
JOB_LOCK = threading.Lock()

def configure(root: Path, etl_runner: Path, now_iso_func) -> None:
    global ROOT, ETL_RUNNER, NOW_ISO
    ROOT = root
    ETL_RUNNER = etl_runner
    NOW_ISO = now_iso_func


def append_job_log(message: str) -> None:
    with JOB_LOCK:
        MONTHLY_JOB["logs"].append(message)
        MONTHLY_JOB["logs"] = MONTHLY_JOB["logs"][-200:]


def run_monthly_job(job_id: str, target_month: str, skip_init: bool) -> None:
    command = [sys.executable, str(ETL_RUNNER), target_month]
    if skip_init:
        command.append("--skip-init")
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")

    append_job_log(f"[{NOW_ISO()}] Starting: {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    assert process.stdout is not None
    for line in process.stdout:
        append_job_log(line.rstrip())

    exit_code = process.wait()
    with JOB_LOCK:
        MONTHLY_JOB["exit_code"] = exit_code
        MONTHLY_JOB["finished_at"] = NOW_ISO()
        MONTHLY_JOB["status"] = "success" if exit_code == 0 else "failed"


def start_monthly_job(target_month: str, skip_init: bool) -> dict:
    with JOB_LOCK:
        if MONTHLY_JOB["status"] == "running":
            raise RuntimeError("A monthly ETL job is already running.")
        job_id = datetime.now().strftime("%Y%m%d%H%M%S")
        MONTHLY_JOB.update(
            {
                "job_id": job_id,
                "status": "running",
                "target_month": target_month,
                "skip_init": skip_init,
                "started_at": NOW_ISO(),
                "finished_at": None,
                "exit_code": None,
                "logs": [],
            }
        )

    thread = threading.Thread(target=run_monthly_job, args=(job_id, target_month, skip_init), daemon=True)
    thread.start()
    with JOB_LOCK:
        return json.loads(json.dumps(MONTHLY_JOB, ensure_ascii=False))
