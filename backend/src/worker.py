import logging
import os
from pathlib import Path
from threading import Event, Thread
from time import sleep

from src.config.settings import ARTIFACTS, cache
from src.services import domain, workflow
from src.services.workflow import LEASE_SECONDS

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'
STEP_DELAY = float(os.getenv('STEP_DELAY_SECONDS', '0'))


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def renew_lease(run_id, token, stop, lost):
    interval = max(1.0, LEASE_SECONDS / 3)
    while not stop.wait(interval):
        try:
            if not workflow.renew(run_id, token, LEASE_SECONDS):
                lost.set()
                return
        except Exception:
            # A transient database error is not proof of takeover; the lease may
            # still expire and be fenced, which the next checkpoint will detect.
            logging.exception('Lease renewal failed for run %s', run_id)


def execute(run_id, token, plan, confirmed, lost):
    """Replay the pinned plan from the last confirmed checkpoint.

    Steps before the checkpoint are skipped instead of replayed, so budget and
    node occupancy are never counted twice. The report is rendered from the whole
    plan, so its digest matches a run that completed in a single pass.
    """
    for step in plan['steps'][confirmed:]:
        if lost.is_set():
            return False
        if STEP_DELAY:
            sleep(STEP_DELAY)
        if not workflow.checkpoint(run_id, token, step, LEASE_SECONDS):
            logging.warning('Attempt fenced out at step %s of run %s', step['index'], run_id)
            return False
    if lost.is_set():
        return False
    # Publication re-checks ownership and that every step is confirmed.
    return workflow.finish(run_id, token, result=domain.report(plan))


def work():
    HEARTBEAT.unlink(missing_ok=True)
    process_stop = Event()
    Thread(target=heartbeat, args=(process_stop,), daemon=True).start()
    while True:
        try:
            job = workflow.claim(LEASE_SECONDS)
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload, plan, confirmed = job
            if not plan:
                plan = domain.build_plan(payload)
            stop, lost = Event(), Event()
            lease = Thread(target=renew_lease, args=(run_id, token, stop, lost), daemon=True)
            lease.start()
            try:
                execute(run_id, token, plan, confirmed, lost)
            except (ValueError, KeyError, TypeError) as exc:
                workflow.finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                workflow.finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
            finally:
                stop.set()
                lease.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
