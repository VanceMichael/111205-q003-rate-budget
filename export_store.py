"""Durable state for an asynchronous partner export.

The store deliberately separates the business identity of a collection
(``intent_key``, derived from tenant + filters) from the partner's task id.
A partner job id is just an attribute of an intent, so a restart can find the
already submitted export instead of creating a second one.
"""

import hashlib
import json
import sqlite3
import time


TERMINAL_STATES = frozenset({'published', 'cancelled', 'failed', 'expired', 'superseded'})


class StoreError(Exception):
    pass


class IntentClosed(StoreError):
    """An operation lost the race against a terminal decision.

    The caller must read the recorded decision instead of retrying.
    """

    def __init__(self, state):
        super().__init__('intent is not open: %s' % state)
        self.state = state


class LinkConflict(StoreError):
    pass


class Conflict(StoreError):
    pass


class DeclarationChanged(StoreError):
    pass


class RowMismatch(StoreError):
    pass


class ColumnMismatch(StoreError):
    pass


class MemberChanged(StoreError):
    pass


class MemberInvalid(StoreError):
    pass


def normalize_filters(filters):
    """JSON-normalize filters so equal filters produce equal intent keys."""
    return json.loads(json.dumps(filters, sort_keys=True, ensure_ascii=False, separators=(',', ':')))


def intent_key(tenant, filters):
    """Business identity: tenant + filters.  A deadline or partner job id is not
    part of it; changing filters is what creates a new intent."""
    return 'v1:' + hashlib.sha256(_base_payload(tenant, filters).encode('utf-8')).hexdigest()


def _base_payload(tenant, filters):
    body = {'tenant': tenant, 'filters': normalize_filters(filters)}
    return json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _attempt_key(base_key, attempt):
    return base_key if attempt == 1 else '%s#%d' % (base_key, attempt)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_key TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    intent_key TEXT NOT NULL UNIQUE,
    tenant TEXT NOT NULL,
    filters_json TEXT NOT NULL,
    deadline REAL NOT NULL,
    state TEXT NOT NULL,
    job_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    cancel_requested_at REAL,
    cancel_source TEXT,
    superseded_by INTEGER,
    published_at REAL,
    terminal_reason TEXT,
    decision_outcome TEXT NOT NULL DEFAULT 'pending',
    decision_at REAL,
    decision_reason TEXT,
    decision_event_id INTEGER,
    FOREIGN KEY (superseded_by) REFERENCES intents(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS intents_job_id ON intents(job_id) WHERE job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS intents_tenant ON intents(tenant, id);

CREATE TABLE IF NOT EXISTS members (
    intent_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    position INTEGER NOT NULL,
    declared_rows INTEGER NOT NULL,
    actual_rows INTEGER,
    columns_json TEXT,
    content_hash TEXT,
    rows_json TEXT,
    downloaded INTEGER NOT NULL DEFAULT 0,
    verified INTEGER NOT NULL DEFAULT 0,
    first_download_at REAL,
    last_download_at REAL,
    PRIMARY KEY (intent_id, name),
    FOREIGN KEY (intent_id) REFERENCES intents(id)
);

CREATE TABLE IF NOT EXISTS links (
    intent_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    issued_at REAL NOT NULL,
    expires_at REAL,
    PRIMARY KEY (intent_id, name),
    FOREIGN KEY (intent_id) REFERENCES intents(id)
);

CREATE TABLE IF NOT EXISTS publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id INTEGER NOT NULL UNIQUE,
    tenant TEXT NOT NULL,
    published_at REAL NOT NULL,
    payload_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (intent_id) REFERENCES intents(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_publication_per_tenant
    ON publications(tenant) WHERE active = 1;
-- The same intent can never publish twice, even under concurrent recovery.
CREATE UNIQUE INDEX IF NOT EXISTS one_publication_per_intent
    ON publications(intent_id);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id INTEGER NOT NULL,
    at REAL NOT NULL,
    kind TEXT NOT NULL,
    detail_json TEXT,
    FOREIGN KEY (intent_id) REFERENCES intents(id)
);
CREATE INDEX IF NOT EXISTS events_intent ON events(intent_id, id);
"""


class ExportStore:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA busy_timeout = 5000')
        return conn

    def _initialize(self):
        conn = self._connect()
        try:
            conn.execute('PRAGMA journal_mode = WAL')
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def _read(self, callback):
        conn = self._connect()
        try:
            return callback(conn)
        finally:
            conn.close()

    def _write(self, callback):
        delay = 0.01
        for attempt in range(8):
            conn = self._connect()
            try:
                conn.execute('BEGIN IMMEDIATE')
                result = callback(conn)
                conn.execute('COMMIT')
                return result
            except sqlite3.OperationalError as error:
                try:
                    conn.execute('ROLLBACK')
                except sqlite3.Error:
                    pass
                if 'locked' in str(error).lower() and attempt < 7:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
            except Exception:
                try:
                    conn.execute('ROLLBACK')
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()
        raise AssertionError('unreachable')

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _event(conn, intent_id, at, kind, detail=None):
        cursor = conn.execute(
            'INSERT INTO events(intent_id, at, kind, detail_json) VALUES (?, ?, ?, ?)',
            (intent_id, at, kind, None if detail is None else _json(detail)),
        )
        return cursor.lastrowid

    @staticmethod
    def _intent_view(row):
        if row is None:
            return None
        intent = dict(row)
        intent['filters'] = json.loads(intent.pop('filters_json'))
        intent['terminal'] = intent['state'] in TERMINAL_STATES
        return intent

    @staticmethod
    def _member_view(row):
        member = dict(row)
        columns = member.pop('columns_json')
        member['columns'] = None if columns is None else json.loads(columns)
        member['downloaded'] = bool(member['downloaded'])
        member['verified'] = bool(member['verified'])
        return member

    @staticmethod
    def _publication_view(row):
        if row is None:
            return None
        publication = dict(row)
        publication['payload'] = json.loads(publication.pop('payload_json'))
        publication['active'] = bool(publication['active'])
        return publication

    @staticmethod
    def _event_view(row):
        event = dict(row)
        detail = event.pop('detail_json')
        event['detail'] = None if detail is None else json.loads(detail)
        return event

    # ------------------------------------------------------------------ intents
    def submit_intent(self, tenant, filters, deadline, now, retry_of=None):
        """Create an intent or return the existing one for the same filters.

        Resubmitting the same filters returns the original intent.  A caller
        that wants to retry a failed/expired collection must say so explicitly
        with ``retry_of``; the new attempt still gets its own durable record
        so the old decision and the new report are never mixed.
        """
        normalized = normalize_filters(filters)
        base_key = intent_key(tenant, normalized)

        def operation(conn):
            if retry_of is None:
                existing = conn.execute('SELECT * FROM intents WHERE base_key = ? ORDER BY attempt DESC LIMIT 1',
                                        (base_key,)).fetchone()
                if existing is not None:
                    self._event(conn, existing['id'], now, 'intent_resubmitted',
                                {'intent_key': existing['intent_key'], 'state': existing['state']})
                    return self._intent_view(existing), False
                previous_attempt = 0
            else:
                previous = conn.execute('SELECT * FROM intents WHERE id = ?', (retry_of,)).fetchone()
                if previous is None:
                    raise StoreError('unknown intent %s' % retry_of)
                if previous['base_key'] != base_key:
                    raise Conflict('retry filters do not match the original intent')
                if previous['state'] not in ('failed', 'expired', 'cancelled'):
                    raise IntentClosed(previous['state'])
                previous_attempt = previous['attempt']

            attempt = previous_attempt + 1
            key = _attempt_key(base_key, attempt)
            cursor = conn.execute(
                'INSERT INTO intents(base_key, attempt, intent_key, tenant, filters_json, deadline, state, '
                'created_at, updated_at, decision_outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (base_key, attempt, key, tenant, _json(normalized), deadline, 'submitted', now, now, 'pending'),
            )
            intent_id = cursor.lastrowid
            self._event(conn, intent_id, now, 'intent_submitted', {
                'intent_key': key, 'deadline': deadline, 'attempt': attempt,
                'retry_of': retry_of,
            })

            older = conn.execute(
                'SELECT id, state FROM intents WHERE tenant = ? AND id <> ? AND base_key <> ? '
                'AND state NOT IN (%s)' % ','.join('?' for _ in TERMINAL_STATES),
                (tenant, intent_id, base_key, *sorted(TERMINAL_STATES)),
            ).fetchall()
            for row in older:
                conn.execute(
                    'UPDATE intents SET state = ?, superseded_by = ?, terminal_reason = ?, updated_at = ?, '
                    "decision_outcome = 'superseded', decision_at = ?, decision_reason = ? WHERE id = ?",
                    ('superseded', intent_id, 'new intent from modified filters', now, now,
                     'filters changed before this report completed', row['id']),
                )
                self._event(conn, row['id'], now, 'intent_superseded', {'by_intent': intent_id, 'previous_state': row['state']})

            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            return self._intent_view(row), True

        return self._write(operation)

    def get_intent(self, intent_id):
        return self._read(lambda conn: self._intent_view(
            conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()))

    def find_intent(self, tenant, filters):
        """Find the current intent for a business filter set."""
        base_key = intent_key(tenant, filters)
        return self._read(lambda conn: self._intent_view(
            conn.execute('SELECT * FROM intents WHERE base_key = ? ORDER BY attempt DESC LIMIT 1',
                         (base_key,)).fetchone()))

    def find_intent_by_key(self, key):
        return self._read(lambda conn: self._intent_view(
            conn.execute('SELECT * FROM intents WHERE intent_key = ?', (key,)).fetchone()))

    def get_intent_by_job(self, job_id):
        return self._read(lambda conn: self._intent_view(
            conn.execute('SELECT * FROM intents WHERE job_id = ?', (job_id,)).fetchone()))

    def supersede(self, intent_id, new_intent_id, now):
        """Retire an intent whose filters were replaced by a newer one."""
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                self._event(conn, intent_id, now, 'supersede_ignored', {'state': row['state']})
                return self._intent_view(row)
            conn.execute(
                'UPDATE intents SET state = ?, superseded_by = ?, terminal_reason = ?, updated_at = ?, '
                "decision_outcome = 'superseded', decision_at = ?, decision_reason = ? WHERE id = ?",
                ('superseded', new_intent_id, 'filters replaced by newer intent', now, now,
                 'a newer report for the same tenant was requested', intent_id),
            )
            self._event(conn, intent_id, now, 'intent_superseded_by_caller',
                        {'by_intent': new_intent_id, 'previous_state': row['state']})
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    def list_intents(self):
        def operation(conn):
            return [self._intent_view(row) for row in conn.execute('SELECT * FROM intents ORDER BY id')]
        return self._read(operation)

    def begin_apply(self, intent_id, now):
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                raise IntentClosed(row['state'])
            if row['job_id'] is not None:
                return self._intent_view(row)
            if row['state'] in ('submitted', 'applying'):
                conn.execute('UPDATE intents SET state = ?, updated_at = ? WHERE id = ?', ('applying', now, intent_id))
                self._event(conn, intent_id, now, 'apply_started', {'intent_key': row['intent_key']})
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    def attach_job(self, intent_id, job_id, now, recovered=False):
        """Record the partner job id, never replacing a different one."""
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['job_id'] is not None and row['job_id'] != job_id:
                raise Conflict('intent %s already maps to %s, refusing %s' % (intent_id, row['job_id'], job_id))
            other = conn.execute('SELECT id FROM intents WHERE job_id = ? AND id <> ?', (job_id, intent_id)).fetchone()
            if other is not None:
                raise Conflict('partner job %s already belongs to intent %s' % (job_id, other['id']))
            state = row['state']
            if state in ('submitted', 'applying'):
                state = 'generating'
            conn.execute('UPDATE intents SET job_id = ?, state = ?, updated_at = ? WHERE id = ?',
                         (job_id, state, now, intent_id))
            self._event(conn, intent_id, now, 'job_recovered' if recovered else 'job_attached',
                        {'job_id': job_id, 'previous_state': row['state']})
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    # ------------------------------------------------------------------ report state
    def record_poll(self, intent_id, now, remote_state):
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                # A concurrent worker already closed this intent.  The caller
                # reads the recorded decision instead of acting on the stale
                # local snapshot.
                self._event(conn, intent_id, now, 'poll_ignored',
                            {'remote_state': remote_state, 'state': row['state']})
                return self._intent_view(row)
            conn.execute('UPDATE intents SET updated_at = ? WHERE id = ?', (now, intent_id))
            self._event(conn, intent_id, now, 'poll', {'remote_state': remote_state, 'job_id': row['job_id']})
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    def record_ready(self, intent_id, members, now):
        """Persist the partner's member declaration for this export."""
        normalized = []
        seen = set()
        for member in members:
            if not isinstance(member, dict) or 'name' not in member or 'rows' not in member:
                raise DeclarationChanged('member declaration is incomplete')
            name = str(member['name'])
            if name in seen:
                raise DeclarationChanged('duplicate member %s' % name)
            seen.add(name)
            rows = member['rows']
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                raise DeclarationChanged('invalid row count for %s' % name)
            normalized.append({'name': name, 'rows': rows})

        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                raise IntentClosed(row['state'])

            existing = conn.execute(
                'SELECT name, declared_rows FROM members WHERE intent_id = ? ORDER BY position', (intent_id,)
            ).fetchall()
            if existing:
                declared = [(item['name'], item['declared_rows']) for item in existing]
                if declared != [(item['name'], item['rows']) for item in normalized]:
                    raise DeclarationChanged('member declaration changed for export %s' % row['job_id'])
            else:
                for position, member in enumerate(normalized):
                    conn.execute(
                        'INSERT INTO members(intent_id, name, position, declared_rows) VALUES (?, ?, ?, ?)',
                        (intent_id, member['name'], position, member['rows']),
                    )
                self._event(conn, intent_id, now, 'members_declared',
                            {'job_id': row['job_id'], 'members': normalized})

            state = row['state']
            if state in ('submitted', 'applying', 'generating', 'ready'):
                state = 'ready'
            conn.execute('UPDATE intents SET state = ?, updated_at = ? WHERE id = ?', (state, now, intent_id))
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    def members(self, intent_id):
        def operation(conn):
            return [self._member_view(row) for row in conn.execute(
                'SELECT * FROM members WHERE intent_id = ? ORDER BY position', (intent_id,))]
        return self._read(operation)

    def record_links(self, intent_id, links, now):
        """Replace addresses for the same report; names must match the declaration."""
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                raise IntentClosed(row['state'])
            declared = {item[0] for item in conn.execute(
                'SELECT name FROM members WHERE intent_id = ?', (intent_id,))}
            names = set()
            for link in links:
                name = link['name']
                if name not in declared:
                    raise LinkConflict('link %r is not a member of export %s' % (name, row['job_id']))
                if name in names:
                    raise LinkConflict('duplicate link for member %s' % name)
                names.add(name)
            for link in links:
                if link.get('job_id') is not None and link.get('job_id') != row['job_id']:
                    raise LinkConflict('link %r belongs to export %s, not %s'
                                       % (link['name'], link.get('job_id'), row['job_id']))
                conn.execute(
                    'INSERT INTO links(intent_id, name, url, issued_at, expires_at) VALUES (?, ?, ?, ?, ?) '
                    'ON CONFLICT(intent_id, name) DO UPDATE SET url = excluded.url, '
                    'issued_at = excluded.issued_at, expires_at = excluded.expires_at',
                    (intent_id, link['name'], link['url'], now, link.get('expires_at')),
                )
            self._event(conn, intent_id, now, 'links_updated', {
                'job_id': row['job_id'],
                'links': [{'name': item['name'], 'expires_at': item.get('expires_at')} for item in links],
            })
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    def links(self, intent_id):
        def operation(conn):
            result = {}
            for row in conn.execute('SELECT * FROM links WHERE intent_id = ?', (intent_id,)):
                result[row['name']] = dict(row)
            return result
        return self._read(operation)

    def record_download(self, intent_id, name, payload, now):
        """Verify one member and record it, in one transaction.

        Cancellation and the local deadline are decided in the same
        transaction as the shard write, so a shard arriving at the same
        virtual instant as a cancel cannot win a race that was already
        decided.  A shard from a different export task is rejected before any
        verification because the member is looked up under its own job.
        """
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                return {'accepted': False, 'reason': row['state'], 'state': row['state']}
            if now >= row['deadline']:
                expired = self._expire(conn, row, now, 'deadline reached while downloading')
                return {'accepted': False, 'reason': 'deadline_reached', 'state': expired['state']}

            if not isinstance(payload, dict) or payload.get('name') != name:
                raise MemberInvalid('downloaded member does not match requested name %s' % name)
            source_job = payload.get('job')
            if source_job is not None and row['job_id'] is not None and source_job != row['job_id']:
                raise MemberInvalid('member %s belongs to export %s, not %s'
                                    % (name, source_job, row['job_id']))
            rows = payload.get('rows')
            if not isinstance(rows, list):
                raise MemberInvalid('member %s has no row list' % name)
            for item in rows:
                if not isinstance(item, dict):
                    raise MemberInvalid('member %s contains a non-object row' % name)

            member = conn.execute(
                'SELECT * FROM members WHERE intent_id = ? AND name = ?', (intent_id, name)
            ).fetchone()
            if member is None:
                raise MemberInvalid('member %s was not declared by this export' % name)

            actual_rows = len(rows)
            if actual_rows != member['declared_rows']:
                raise RowMismatch('member %s declared %s rows but delivered %s'
                                  % (name, member['declared_rows'], actual_rows))
            # Column order may differ between files of the same export.  It is
            # preserved per member (from the first row) and only the order
            # inside one member must be stable; row content is rebuilt by key,
            # so no consumer has to assume one global order.
            column_order = list(rows[0].keys()) if rows else []
            for item in rows:
                if list(item.keys()) != column_order:
                    raise ColumnMismatch('member %s has inconsistent columns across rows' % name)
            columns = column_order
            digest = hashlib.sha256(_json(rows).encode('utf-8')).hexdigest()

            if member['downloaded']:
                if member['content_hash'] != digest:
                    raise MemberChanged('member %s changed after its address was renewed' % name)
                return {'accepted': False, 'reason': 'already_downloaded', 'state': row['state']}

            conn.execute(
                'UPDATE members SET actual_rows = ?, columns_json = ?, content_hash = ?, rows_json = ?, '
                'downloaded = 1, verified = 1, first_download_at = ?, last_download_at = ? '
                'WHERE intent_id = ? AND name = ?',
                (actual_rows, _json(columns), digest, _json(rows), now, now, intent_id, name),
            )
            remaining = conn.execute(
                'SELECT COUNT(*) AS n FROM members WHERE intent_id = ? AND verified = 0', (intent_id,)
            ).fetchone()['n']
            new_state = 'verifying' if remaining == 0 else 'downloading'
            conn.execute('UPDATE intents SET state = ?, updated_at = ? WHERE id = ?', (new_state, now, intent_id))
            self._event(conn, intent_id, now, 'member_verified', {
                'name': name, 'declared_rows': member['declared_rows'], 'actual_rows': actual_rows,
                'columns': columns, 'job_id': row['job_id'],
            })
            return {
                'accepted': True,
                'state': new_state,
                'member': {'name': name, 'rows': actual_rows, 'columns': columns},
            }
        return self._write(operation)

    def _expire(self, conn, row, now, reason):
        self._event(conn, row['id'], now, 'deadline_reached', {'reason': reason, 'deadline': row['deadline']})
        conn.execute(
            'UPDATE intents SET state = ?, updated_at = ?, terminal_reason = ?, '
            "decision_outcome = 'expired', decision_at = ?, decision_reason = ? WHERE id = ?",
            ('expired', now, reason, now, reason, row['id']),
        )
        return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (row['id'],)).fetchone())

    def mark_expired(self, intent_id, now, reason='local deadline reached'):
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                self._event(conn, intent_id, now, 'expiry_ignored', {'state': row['state'], 'reason': reason})
                return self._intent_view(row)
            return self._expire(conn, row, now, reason)
        return self._write(operation)

    def mark_failed(self, intent_id, now, reason, detail=None):
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] in TERMINAL_STATES:
                self._event(conn, intent_id, now, 'failure_ignored',
                            {'state': row['state'], 'reason': reason, 'detail': detail})
                return self._intent_view(row)
            event_id = self._event(conn, intent_id, now, 'export_failed',
                                   {'reason': reason, 'detail': detail})
            conn.execute(
                'UPDATE intents SET state = ?, updated_at = ?, terminal_reason = ?, '
                "decision_outcome = 'failed', decision_at = ?, decision_reason = ?, decision_event_id = ? "
                'WHERE id = ?',
                ('failed', now, reason, now, reason, event_id, intent_id),
            )
            return self._intent_view(conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
        return self._write(operation)

    # ------------------------------------------------------------------ decisions
    def request_cancel(self, intent_id, now, source='user'):
        """Linearizable cancellation.  A cancel is accepted only while the intent
        is still open; after publication it is explicitly rejected and the
        decision record explains why."""
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)

            if row['state'] == 'published':
                event_id = self._event(conn, intent_id, now, 'cancel_rejected', {
                    'source': source,
                    'reason': 'publication committed before cancellation',
                    'publish_event_id': row['decision_event_id'],
                    'published_at': row['published_at'],
                })
                return {
                    'accepted': False,
                    'decision': {
                        'outcome': 'published', 'at': row['published_at'],
                        'reason': row['decision_reason'], 'event_id': row['decision_event_id'],
                    },
                    'cancel_event_id': event_id,
                    'state': row['state'],
                }
            if row['state'] == 'cancelled':
                event_id = self._event(conn, intent_id, now, 'cancel_repeated', {'source': source})
                return {
                    'accepted': False,
                    'decision': {
                        'outcome': 'cancelled', 'at': row['decision_at'],
                        'reason': row['decision_reason'], 'event_id': row['decision_event_id'],
                    },
                    'cancel_event_id': event_id,
                    'state': row['state'],
                }
            if row['state'] in TERMINAL_STATES:
                event_id = self._event(conn, intent_id, now, 'cancel_rejected', {
                    'source': source, 'reason': 'intent already %s' % row['state'],
                })
                return {
                    'accepted': False,
                    'decision': {
                        'outcome': row['decision_outcome'], 'at': row['decision_at'],
                        'reason': row['decision_reason'], 'event_id': row['decision_event_id'],
                    },
                    'cancel_event_id': event_id,
                    'state': row['state'],
                }

            event_id = self._event(conn, intent_id, now, 'cancel_accepted', {
                'source': source,
                'reason': 'cancellation committed before publication',
                'previous_state': row['state'],
            })
            conn.execute(
                'UPDATE intents SET state = ?, updated_at = ?, cancel_requested_at = ?, cancel_source = ?, '
                "terminal_reason = ?, decision_outcome = 'cancelled', decision_at = ?, "
                'decision_reason = ?, decision_event_id = ? WHERE id = ?',
                ('cancelled', now, now, source, '%s cancellation' % source, now,
                 'cancellation committed before publication', event_id, intent_id),
            )
            updated = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            return {
                'accepted': True,
                'decision': {
                    'outcome': 'cancelled', 'at': now,
                    'reason': 'cancellation committed before publication', 'event_id': event_id,
                },
                'cancel_event_id': event_id,
                'state': updated['state'],
            }
        return self._write(operation)

    def finalize(self, intent_id, now):
        """Publish exactly once, only after every declared member is verified."""
        def operation(conn):
            row = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            if row is None:
                raise StoreError('unknown intent %s' % intent_id)
            if row['state'] == 'published':
                event_id = self._event(conn, intent_id, now, 'publish_already_committed',
                                       {'published_at': row['published_at']})
                return {
                    'outcome': 'published', 'already': True,
                    'decision': self._decision_view(row),
                    'publication': self._publication_view(conn.execute(
                        'SELECT * FROM publications WHERE intent_id = ?', (intent_id,)).fetchone()),
                }
            if row['state'] in TERMINAL_STATES:
                return {'outcome': row['state'], 'already': True, 'decision': self._decision_view(row)}

            if now >= row['deadline']:
                expired = self._expire(conn, row, now, 'local deadline reached before publication')
                return {'outcome': 'expired', 'already': False, 'decision': self._decision_view(expired)}

            members = conn.execute(
                'SELECT * FROM members WHERE intent_id = ? ORDER BY position', (intent_id,)
            ).fetchall()
            if not members:
                return {'outcome': 'pending', 'already': False, 'reason': 'no member declaration',
                        'decision': self._decision_view(row)}
            missing = [member['name'] for member in members if not member['downloaded'] or not member['verified']]
            if missing:
                return {'outcome': 'pending', 'already': False, 'reason': 'members incomplete',
                        'missing': missing, 'decision': self._decision_view(row)}
            mismatched = [member['name'] for member in members
                          if member['actual_rows'] != member['declared_rows']]
            if mismatched:
                raise RowMismatch('verified members disagree with declaration: %s' % ', '.join(mismatched))
            if row['superseded_by'] is not None:
                return {'outcome': 'superseded', 'already': True, 'decision': self._decision_view(row)}

            # A newer intent with different filters may have been submitted
            # concurrently.  If it committed first, submit_intent marked this
            # one superseded; this backstop covers a row created between the
            # checks and keeps the published report from being the stale one.
            newer = conn.execute(
                'SELECT id FROM intents WHERE tenant = ? AND id > ? AND base_key <> ? '
                'AND state NOT IN (%s) LIMIT 1' % ','.join('?' for _ in TERMINAL_STATES),
                (row['tenant'], intent_id, row['base_key'], *sorted(TERMINAL_STATES)),
            ).fetchone()
            if newer is not None:
                return {'outcome': 'superseded', 'already': False, 'superseded_by': newer['id'],
                        'decision': self._decision_view(row)}

            payload_members = []
            records = []
            total = 0
            for member in members:
                total += member['actual_rows']
                columns = json.loads(member['columns_json']) if member['columns_json'] else []
                payload_members.append({
                    'name': member['name'], 'rows': member['actual_rows'], 'columns': columns,
                })
                records.extend(self._rows_for_member(conn, intent_id, member['name']))

            payload = {
                'intent_key': row['intent_key'],
                'tenant': row['tenant'],
                'filters': json.loads(row['filters_json']),
                'export_job': row['job_id'],
                'published_at': now,
                'members': payload_members,
                'rows_total': total,
                'records': records,
            }
            event_id = self._event(conn, intent_id, now, 'published', {
                'job_id': row['job_id'], 'members': payload_members, 'rows_total': total,
            })
            conn.execute('UPDATE publications SET active = 0 WHERE tenant = ? AND active = 1', (row['tenant'],))
            conn.execute(
                'INSERT INTO publications(intent_id, tenant, published_at, payload_json, active) '
                'VALUES (?, ?, ?, ?, 1)',
                (intent_id, row['tenant'], now, _json(payload)),
            )
            conn.execute(
                'UPDATE intents SET state = ?, updated_at = ?, published_at = ?, '
                "decision_outcome = 'published', decision_at = ?, decision_reason = ?, "
                'decision_event_id = ?, terminal_reason = NULL WHERE id = ?',
                ('published', now, now, now, 'all declared members verified', event_id, intent_id),
            )
            updated = conn.execute('SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone()
            publication = conn.execute('SELECT * FROM publications WHERE intent_id = ?', (intent_id,)).fetchone()
            return {
                'outcome': 'published', 'already': False,
                'decision': self._decision_view(updated),
                'publication': self._publication_view(publication),
            }
        return self._write(operation)

    def _rows_for_member(self, conn, intent_id, name):
        # Rows are stored verbatim next to the download hash so the published
        # payload is exactly the verified content and a changed report is
        # detectable through the hash.
        row = conn.execute('SELECT rows_json FROM members WHERE intent_id = ? AND name = ?',
                           (intent_id, name)).fetchone()
        return [] if row is None or row['rows_json'] is None else json.loads(row['rows_json'])

    @staticmethod
    def _decision_view(intent_or_row):
        if not isinstance(intent_or_row, dict):
            intent_or_row = dict(intent_or_row)
        return {
            'outcome': intent_or_row.get('decision_outcome', 'pending'),
            'at': intent_or_row.get('decision_at'),
            'reason': intent_or_row.get('decision_reason'),
            'event_id': intent_or_row.get('decision_event_id'),
            'state': intent_or_row.get('state'),
        }

    def publication_for_intent(self, intent_id):
        def operation(conn):
            return self._publication_view(conn.execute(
                'SELECT * FROM publications WHERE intent_id = ?', (intent_id,)).fetchone())
        return self._read(operation)

    def active_publication(self, tenant):
        def operation(conn):
            return self._publication_view(conn.execute(
                'SELECT * FROM publications WHERE tenant = ? AND active = 1', (tenant,)).fetchone())
        return self._read(operation)

    def list_publications(self):
        def operation(conn):
            return [self._publication_view(row) for row in conn.execute(
                'SELECT * FROM publications ORDER BY id')]
        return self._read(operation)

    # ------------------------------------------------------------------ audit
    def record_event(self, intent_id, now, kind, detail=None):
        def operation(conn):
            return self._event(conn, intent_id, now, kind, detail)
        return self._write(operation)

    def max_event_id(self):
        return self._read(lambda conn: conn.execute('SELECT COALESCE(MAX(id), 0) FROM events').fetchone()[0])

    def events_since(self, event_id):
        def operation(conn):
            return [self._event_view(row) for row in conn.execute(
                'SELECT * FROM events WHERE id > ? ORDER BY id', (event_id,))]
        return self._read(operation)

    def events(self, intent_id=None):
        def operation(conn):
            if intent_id is None:
                rows = conn.execute('SELECT * FROM events ORDER BY id')
            else:
                rows = conn.execute('SELECT * FROM events WHERE intent_id = ? ORDER BY id', (intent_id,))
            return [self._event_view(row) for row in rows]
        return self._read(operation)

    def snapshot(self, intent_id):
        def operation(conn):
            intent = self._intent_view(conn.execute(
                'SELECT * FROM intents WHERE id = ?', (intent_id,)).fetchone())
            return {
                'intent': intent,
                'members': [self._member_view(row) for row in conn.execute(
                    'SELECT * FROM members WHERE intent_id = ? ORDER BY position', (intent_id,))],
                'links': {row['name']: dict(row) for row in conn.execute(
                    'SELECT * FROM links WHERE intent_id = ?', (intent_id,))},
                'publication': self._publication_view(conn.execute(
                    'SELECT * FROM publications WHERE intent_id = ?', (intent_id,)).fetchone()),
                'active_publication': self._publication_view(conn.execute(
                    'SELECT * FROM publications WHERE tenant = ? AND active = 1',
                    (intent['tenant'] if intent else None,)).fetchone()),
                'events': [self._event_view(row) for row in conn.execute(
                    'SELECT * FROM events WHERE intent_id = ? ORDER BY id', (intent_id,))],
            }
        return self._read(operation)
