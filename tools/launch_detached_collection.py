"""Launch one bounded Windows collection outside the caller's process job.

No resume loop and no VM recreation. The collector retains its own budget,
checkpoint/export and stop/billing finally block. --check starts no browser.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path)
    p.add_argument('--cookies-file', type=Path, default=ROOT / '.cookies.json')
    p.add_argument('--logs', type=Path, required=True)
    p.add_argument('--check', action='store_true')
    p.add_argument('--detach-check', action='store_true', help='Exit before the harmless check child finishes')
    p.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    a = p.parse_args()
    a.check = a.check or a.detach_check
    if os.name != 'nt':
        p.error('This launcher explicitly verifies Windows job breakaway')
    if not a.check and (a.input is None or not a.input.is_file()):
        p.error('--input must name an existing profile')
    a.logs.mkdir(parents=True, exist_ok=True)
    if a.worker:
        from instagram_scraper.cli import main as collect
        from instagram_scraper.complete import CompleteCollector
        started = time.time()
        code = 1
        original_execute = CompleteCollector._execute
        recorded_timeouts = set()
        def measured_execute(collector, job):
            try:
                return original_execute(collector, job)
            finally:
                if (job.get('payload', {}).get('stopReason') == 'PageFetchReadTimeoutError'
                        and job['id'] not in recorded_timeouts):
                    recorded_timeouts.add(job['id'])
                    try:
                        event = {'type':'confirmed_page_fetch_timeout', 'at':time.time(),
                                 'jobId':job['id'], 'records':collector.state.count()}
                        with (a.logs / 'collector.events.jsonl').open('a', encoding='utf-8') as stream:
                            stream.write(json.dumps(event) + '\n')
                    except Exception as error:
                        print('Could not record timeout snapshot: ' + type(error).__name__, file=sys.stderr)
        CompleteCollector._execute = measured_execute
        try:
            code = collect(['--input', str(a.input.resolve()), '--cookies-file', str(a.cookies_file.resolve())])
        finally:
            CompleteCollector._execute = original_execute
            end = {'pid': os.getpid(), 'startedAt': started, 'finishedAt': time.time(), 'exitCode': code}
            temporary = a.logs / 'collector.exit.tmp'
            temporary.write_text(json.dumps(end, indent=2) + '\n', encoding='utf-8')
            temporary.replace(a.logs / 'collector.exit.json')
        raise SystemExit(code)
    if a.check:
        script = (
            "import ctypes,json,os,time; k=ctypes.windll.kernel32; "
            "k.GetCurrentProcess.restype=ctypes.c_void_p; inside=ctypes.c_int(); "
            "ok=k.IsProcessInJob(ctypes.c_void_p(k.GetCurrentProcess()),None,ctypes.byref(inside)); "
            "print(json.dumps({'pid':os.getpid(),'checked':bool(ok),'inJob':bool(inside.value)}),flush=True); "
            "time.sleep(5); print(json.dumps({'survived':True}),flush=True)"
        )
        args = [sys.executable, '-c', script]
    else:
        config = json.loads(a.input.read_text(encoding='utf-8-sig'))
        if not 0 < float(config.get('maxRunSeconds', 0)) < float(config.get('broSessionTimeout', 0)) <= 21600:
            p.error('Require an explicit collection budget below a VM timeout of at most 21600 seconds')
        args = [sys.executable, str(Path(__file__).resolve()), '--worker', '--input', str(a.input.resolve()),
                '--cookies-file', str(a.cookies_file.resolve()), '--logs', str(a.logs.resolve())]
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_BREAKAWAY_FROM_JOB
    stem = 'check' if a.check else 'collector'
    with (a.logs / f'{stem}.stdout.log').open('xb') as out, (a.logs / f'{stem}.stderr.log').open('xb') as err:
        # Fail explicitly if breakaway is forbidden; do not silently launch
        # a paid browser under the same vulnerable parent process job.
        child = subprocess.Popen(args, cwd=ROOT, stdin=subprocess.DEVNULL,
                                 stdout=out, stderr=err, creationflags=flags, close_fds=True)
    record = {'pid': child.pid, 'startedAt': time.time(), 'breakawayRequested': True,
              'input': str(a.input.resolve()) if a.input else None, 'check': a.check}
    inside = ctypes.c_int()
    checked = ctypes.windll.kernel32.IsProcessInJob(ctypes.c_void_p(int(child._handle)), None, ctypes.byref(inside))
    record.update(launcherJobChecked=bool(checked), launcherInJob=bool(inside.value))
    (a.logs / f'{stem}.process.json').write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
    if not checked or inside.value:
        child.terminate()
        child.wait(timeout=15)
        raise RuntimeError('Launch remained in a process job; use the verified external launcher')
    if a.check and not a.detach_check:
        record['exitCode'] = child.wait(timeout=15)
        details = json.loads((a.logs / 'check.stdout.log').read_text().splitlines()[0])
        # The venv redirector may place its interpreter in its own job;
        # check the process we launched, not that redirector's child.
        if record['exitCode'] or not checked or inside.value:
            raise RuntimeError('Detached launch did not confirm escape from the caller job')
        record.update(details)
    print(json.dumps(record))


if __name__ == '__main__':
    main()
