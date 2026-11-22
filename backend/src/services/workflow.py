import hashlib
import os
import uuid

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, select

from src.config.settings import cache, engine
from src.constants.workflow import Status
from src.models.tables import audit, cases, checkpoints, evidence, runs
from src.services import domain
from src.utils.encoding import canonical
from src.utils.time import utc_time

# A run holds an expiring lease on its attempt. The token below is also a fencing
# token: every progress write and the final publication must present it, so a
# worker that comes back after another worker took over can no longer write.
LEASE_SECONDS = int(os.getenv('RUN_LEASE_SECONDS', '30'))
MAX_ATTEMPTS = int(os.getenv('RUN_MAX_ATTEMPTS', '5'))


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def recent_runs():
    # The pinned plan, fencing token and lease are internal; only show progress.
    hidden = {'plan', 'attempt_token', 'lease_expires_at'}
    return [{key: value for key, value in row.items() if key not in hidden} for row in recent(runs)]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def enqueue(case_id, actor):
    with engine.begin() as conn:
        found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass
    return result


def _load_plan(conn, run):
    # The plan is pinned on first claim and reused on every resume. Recomputing it
    # would be equivalent (it is a pure function of the payload), but pinning keeps
    # the step order, byte accounting and report digest identical across attempts.
    plan = run.plan
    if not plan:
        plan = domain.build_plan(found(conn, cases, run.case_id)['payload'])
        conn.execute(runs.update().where(runs.c.id == run.id).values(plan=plan))
    return plan


def claim(lease_seconds=LEASE_SECONDS):
    """Take the next runnable job and fence any previous attempt at it."""
    now = utc_time()
    ready = or_(runs.c.status == Status.QUEUED,
                and_(runs.c.status == Status.RUNNING,
                     or_(runs.c.lease_expires_at.is_(None), runs.c.lease_expires_at < now)))
    with engine.begin() as conn:
        run = conn.execute(select(runs).where(ready).order_by(runs.c.id)
                           .limit(1).with_for_update(skip_locked=True)).mappings().first()
        if not run:
            return None
        attempts = (run.progress or {}).get('attempts', 0) + 1
        if attempts > MAX_ATTEMPTS:
            # Repeated takeovers without progress: stop reviving this run so a
            # poison input cannot loop forever. Confirmed checkpoints are kept.
            conn.execute(runs.update().where(runs.c.id == run.id).values(
                status=Status.FAILED, finished_at=utc_time(), lease_expires_at=None,
                error='多次接手仍未能完成，请检查输入后重新提交试算。'))
            log(conn, 'worker', Status.FAILED.value, run.id)
            return None
        token = str(uuid.uuid4())
        try:
            plan = _load_plan(conn, run)
        except (ValueError, TypeError) as exc:
            conn.execute(runs.update().where(runs.c.id == run.id).values(
                status=Status.FAILED, finished_at=utc_time(), lease_expires_at=None,
                error=str(exc)[:500]))
            log(conn, 'worker', Status.FAILED.value, run.id)
            return None
        done = conn.execute(select(func.count()).select_from(checkpoints)
                            .where(checkpoints.c.run_id == run.id)).scalar_one()
        conn.execute(runs.update().where(runs.c.id == run.id).values(
            started_at=run.started_at or utc_time(), status=Status.RUNNING, attempt_token=token,
            lease_expires_at=utc_time(lease_seconds),
            progress={'confirmed': done, 'total': len(plan['steps']), 'attempts': attempts}))
        payload = found(conn, cases, run.case_id)['payload']
        return run.id, token, payload, plan, done


def owned(conn, run_id, token):
    """Lock the run row and confirm this attempt still holds an unexpired lease."""
    run = conn.execute(select(runs).where(runs.c.id == run_id).with_for_update()).mappings().first()
    if (run is None or run.status != Status.RUNNING or run.attempt_token != token
            or not run.lease_expires_at or run.lease_expires_at < utc_time()):
        return None
    return run


def renew(run_id, token, lease_seconds=LEASE_SECONDS):
    with engine.begin() as conn:
        run = owned(conn, run_id, token)
        if run is None:
            return False
        conn.execute(runs.update().where(runs.c.id == run_id, runs.c.attempt_token == token)
                     .values(lease_expires_at=utc_time(lease_seconds)))
        return True


def checkpoint(run_id, token, step, lease_seconds=LEASE_SECONDS):
    """Confirm one executed step and extend the lease in the same transaction.

    Returns False when this attempt has been fenced out. Confirmed steps are keyed
    by (run_id, step_index): a resumed attempt continues after them instead of
    replaying the move, so budget and node occupancy are never counted twice.
    """
    with engine.begin() as conn:
        run = owned(conn, run_id, token)
        if run is None:
            return False
        done = conn.execute(select(func.count()).select_from(checkpoints)
                            .where(checkpoints.c.run_id == run_id)).scalar_one()
        if step['index'] < done:
            # Already confirmed by this or an earlier attempt; do not reapply.
            return True
        if step['index'] != done:
            return False
        existing = conn.execute(select(checkpoints.c.id).where(
            checkpoints.c.run_id == run_id, checkpoints.c.step_index == step['index'])).first()
        if existing is None:
            conn.execute(checkpoints.insert().values(
                run_id=run_id, step_index=step['index'], batch_id=step['batch_id'],
                source_node=step['source'], target_node=step['target'], size_bytes=step['size'],
                attempt_token=token, confirmed_at=utc_time()))
        total = run.progress.get('total') if run.progress else None
        attempts = run.progress.get('attempts', 1) if run.progress else 1
        conn.execute(runs.update().where(runs.c.id == run_id, runs.c.attempt_token == token).values(
            lease_expires_at=utc_time(lease_seconds),
            progress={'confirmed': done + 1, 'total': total, 'attempts': attempts,
                      'last_batch_id': step['batch_id']}))
        return True


def finish(run_id, token, result=None, error=None):
    """Publish the result. Only the current attempt may complete the run."""
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        run = owned(conn, run_id, token)
        if run is None:
            return False
        if result is not None:
            plan = run.plan or {}
            steps = plan.get('steps', [])
            done = conn.execute(select(func.count()).select_from(checkpoints)
                                .where(checkpoints.c.run_id == run_id)).scalar_one()
            if done != len(steps):
                # Never publish a partial report as complete; let a resume finish it.
                return False
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error,
                                lease_expires_at=None)).rowcount
        if not count:
            return False
        if result is not None:
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
        log(conn, 'worker', status.value, run_id)
        return True


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        found(conn, evidence, identity)
        count = conn.execute(evidence.update().where(evidence.c.id == identity, evidence.c.reviewed_at.is_(None))
                            .values(review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
        if count != 1:
            raise HTTPException(409, '该证据已经复核')
        log(conn, actor, 'review', identity)
        return found(conn, evidence, identity)
