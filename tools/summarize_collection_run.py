"""Read-only audit of one comment run; rejects stale/copied OUTPUT evidence."""
import argparse
import json
from pathlib import Path
import sqlite3


def summarize(directory, logs=None):
    directory = Path(directory).resolve()
    db = sqlite3.connect((directory / 'state.sqlite').as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    meta = {row['key']: json.loads(row['value']) for row in db.execute(
        "SELECT key,value FROM meta WHERE key IN ('sessionId','invocationBaseline','invocationMetrics','datasetName')")}
    counts = [dict(row) for row in db.execute('''SELECT r.scope,
        (SELECT json_extract(j.payload,'$.code') FROM jobs j WHERE j.scope=r.scope AND j.kind='parents' LIMIT 1) AS code,
        count(*) AS records,
        sum(json_extract(r.payload,'$.parentCommentId') IS NULL) AS parents,
        sum(json_extract(r.payload,'$.parentCommentId') IS NOT NULL) AS replies
        FROM records r GROUP BY r.scope''')]
    ids = {str(row[0]) for row in db.execute("SELECT json_extract(payload,'$.id') FROM records")}
    count = sum(row['records'] for row in counts)
    operations = dict(db.execute('SELECT state,count(*) FROM command_operations GROUP BY state'))
    active = [dict(row) for row in db.execute("SELECT operation_kind,state,created_at,updated_at FROM command_operations "
        "WHERE state NOT IN ('applied','failed','deferred')")]
    reasons = dict(db.execute("SELECT coalesce(json_extract(payload,'$.stopReason'),'pending'),count(*) "
        "FROM jobs WHERE status!='done' GROUP BY 1"))
    db.close()
    result = dict(sessionId=meta.get('sessionId'), count=count, uniqueIds=len(ids), perPost=counts,
                  operations=operations, activeOperations=active, unfinishedReasons=reasons,
                  invocationMetrics=meta.get('invocationMetrics', {}))
    
    dataset_name = meta.get('datasetName', 'default')
    export = directory / 'datasets' / dataset_name / 'items.jsonl'
    
    if export.exists():
        exported = set()
        lines = 0
        with export.open(encoding='utf-8') as source:
            for line in source:
                if line.strip():
                    exported.add(str(json.loads(line)['id']))
                    lines += 1
        result['export'] = dict(rows=lines, uniqueIds=len(exported), sameIds=exported == ids,
                                noDuplicates=len(exported) == lines)
    output = directory / 'key_value_stores' / dataset_name / 'OUTPUT.json'
    if output.exists():
        data = json.loads(output.read_text(encoding='utf-8'))
        sessions = (data.get('billing') or {}).get('sessions', [])
        matches = any(s.get('sessionId') == meta.get('sessionId') for s in sessions)
        recent = output.stat().st_mtime >= meta.get('invocationBaseline', {}).get('startedAt', 0) - 1
        result['outputFresh'] = matches and recent
        if result['outputFresh']:
            result.update(budget=data.get('budget'), billing=data.get('billing'),
                          stopReason=data.get('stopReason'), deepCollection=data.get('deepCollection'),
                          metrics=data.get('metrics'))
        else:
            result['outputWarning'] = 'OUTPUT session or timestamp does not match this invocation; not used'
    if logs:
        logs = Path(logs)
        terminal = logs / 'collector.exit.json'
        result['workerTerminated'] = terminal.exists()
        if terminal.exists():
            result['workerExit'] = json.loads(terminal.read_text(encoding='utf-8'))
        events_path = logs / 'collector.events.jsonl'
        if events_path.exists():
            events = [json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            cancelled = [event for event in events if event.get('type') == 'confirmed_page_fetch_timeout']
            result['confirmedCancellationEvents'] = cancelled
            if cancelled:
                result['recordsAfterFirstCancellation'] = max(0, count - cancelled[0]['records'])
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path)
    p.add_argument('--output', type=Path)
    p.add_argument('--logs', type=Path)
    a = p.parse_args()
    text = json.dumps(summarize(a.directory, a.logs), ensure_ascii=False, indent=2)
    if a.output:
        a.output.write_text(text + '\n', encoding='utf-8')
    else:
        print(text)
