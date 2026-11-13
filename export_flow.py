"""Partner-facing adapter and the offline progress engine.

The engine never sleeps and never reads a real clock: every call receives the
virtual time from the caller.  All durable decisions are delegated to
:class:`export_store.ExportStore`; this module only talks to the partner and
interprets its answers.
"""

import inspect
from urllib.parse import parse_qs, urlsplit

import export_store


class PartnerUnavailable(Exception):
    """The partner could not be reached or gave an unusable answer."""


class PartnerReportFailed(Exception):
    """The partner reports that this export failed to generate."""


class PartnerReportCancelled(Exception):
    """The partner reports that this export was cancelled."""


class ExportLinksUnavailable(Exception):
    """Links for this export cannot be issued yet (e.g. no declaration)."""


class LinkExpired(TimeoutError):
    pass


class MemberNotReady(FileNotFoundError):
    pass


class PartnerAdapter:
    """Small protocol layer over ExportAPI/fake HTTP clients.

    It exposes the partner's vocabulary (task ids, addresses, member lists)
    and nothing about business identity.  ``intent_key`` is passed only so a
    synthetic backend can make a repeated apply idempotent across a crash;
    a real backend would instead accept an idempotency key.
    """

    def __init__(self, api, address_job=None):
        self.api = api
        self.address_job = address_job or default_address_job

    def apply(self, tenant, filters, now, intent_key=None):
        """Submit the export.

        Synthetic backends accept an idempotency key so that a crash between
        the partner apply and the local job-id write can be recovered without
        submitting a second report.  Backends without that parameter are called
        unchanged.
        """
        try:
            parameters = inspect.signature(self.api.apply).parameters
        except (TypeError, ValueError):
            parameters = {}
        if intent_key is not None and 'intent_key' in parameters:
            return self.api.apply(tenant, filters, now, intent_key=intent_key)
        return self.api.apply(tenant, filters, now)

    def status(self, job_id, now):
        try:
            response = self.api.status(job_id, now)
        except KeyError as error:
            raise PartnerUnavailable('partner does not know job %s' % job_id) from error
        state = response.get('state')
        if state == 'failed':
            raise PartnerReportFailed('partner reports job %s failed' % job_id)
        if state == 'cancelled':
            raise PartnerReportCancelled('partner reports job %s cancelled' % job_id)
        if state == 'ready':
            members = response.get('members')
            if not isinstance(members, list):
                raise PartnerUnavailable('ready export has no member declaration')
            return 'ready', [{'name': str(item['name']), 'rows': int(item['rows'])} for item in members]
        if state not in ('generating', 'pending', 'queued'):
            raise PartnerUnavailable('unknown partner state %r' % state)
        return 'generating', None

    def links(self, job_id, now):
        try:
            addresses = self.api.links(job_id, now)
        except ValueError as error:
            raise ExportLinksUnavailable(str(error)) from error
        except KeyError as error:
            raise PartnerUnavailable('partner does not know job %s' % job_id) from error
        if not isinstance(addresses, list):
            raise PartnerUnavailable('partner returned no address list')
        result = []
        for address in addresses:
            owner = self.address_job(address)
            if owner is not None and owner != job_id:
                raise LinkConflict(
                    'address for %s belongs to export %s, not %s' % (address, owner, job_id))
            result.append({'name': _member_from_address(address), 'url': address,
                           'expires_at': _expiry_from_address(address), 'job_id': owner})
        return result

    def download(self, link, now):
        try:
            return self.api.download(link, now)
        except FileNotFoundError as error:
            raise MemberNotReady(str(error)) from error
        except TimeoutError as error:
            raise LinkExpired(str(error)) from error
        except (ValueError, KeyError) as error:
            raise PartnerUnavailable(str(error)) from error

    def cancel(self, job_id):
        try:
            self.api.cancel(job_id)
        except (ValueError, KeyError) as error:
            raise PartnerUnavailable(str(error)) from error


class LinkConflict(export_store.LinkConflict):
    """The partner returned an address for a different export task."""


def default_address_job(address):
    """Best-effort extraction of the owning task from an address.

    The synthetic backend encodes the job as ``local://<job>/<member>``; real
    partners usually sign opaque URLs, in which case the caller can supply a
    parser via ``address_job``.  Returning None means "cannot tell", which
    falls back to the declaration check.
    """
    parsed = urlsplit(address)
    if parsed.scheme == 'local' and parsed.netloc:
        return parsed.netloc
    return None


def _member_from_address(address):
    return urlsplit(address).path.lstrip('/')


def _job_from_address(address):
    return default_address_job(address)


def _expiry_from_address(address):
    try:
        return int(parse_qs(urlsplit(address).query)['expires'][0])
    except (KeyError, ValueError, IndexError):
        return None


class ExportEngine:
    """Drives one intent through apply → poll → links → download → publish.

    The same engine is used by the demo and by tests, so the restart cases
    exercised in the demo are the same code paths used in production.
    """

    def __init__(self, api, store):
        self.partner = PartnerAdapter(api)
        self.store = store

    # ------------------------------------------------------------------ intent
    def submit(self, tenant, filters, deadline, now):
        intent, created = self.store.submit_intent(tenant, filters, deadline, now)
        self.close_expired(now)
        return intent, created

    def close_expired(self, now):
        for intent in self.store.list_intents():
            if intent['state'] not in ('published', 'cancelled', 'failed', 'expired', 'superseded') \
                    and now >= intent['deadline']:
                self.store.mark_expired(intent['id'], now, 'local deadline reached')

    def retry(self, intent_id, deadline, now):
        """Start a new attempt for the same filters after a failed/expired one.

        The new attempt has its own local intent, job id and publication.  It
        never reuses or mixes with the old report.
        """
        previous = self.store.get_intent(intent_id)
        if previous is None:
            raise export_store.StoreError('unknown intent %s' % intent_id)
        intent, created = self.store.submit_intent(
            previous['tenant'], previous['filters'], deadline, now, retry_of=intent_id)
        return intent, created

    def supersede(self, intent_id, new_intent_id, now):
        """Retire an old intent after its filters were replaced.

        Called by ``submit`` for the same tenant: the new intent is the
        business truth, so the old one must not publish late.  The store
        performs this atomically inside ``submit_intent``; this hook exists
        for tests and for callers that create intents elsewhere.
        """
        return self.store.supersede(intent_id, new_intent_id, now)

    def ensure_submitted(self, intent_id, now):
        """Find or create the partner task for an intent.

        A job id already recorded locally wins.  If the process died between
        the partner apply and the local write, the backend's idempotent apply
        returns the original task instead of creating a second one.
        """
        intent = self.store.get_intent(intent_id)
        if intent['job_id'] is not None:
            return intent
        return self._apply(intent, now)

    def recover(self, intent_id, now):
        """Resume a persisted intent after a restart.

        Returns the first step that can make progress: refresh a missing
        address set, finish incomplete members, or publish.  An intent that
        already reached a terminal decision is left untouched.
        """
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'intent': intent}
        if now >= intent['deadline']:
            return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}

        if intent['state'] in ('submitted', 'applying') or intent['job_id'] is None:
            intent = self.ensure_submitted(intent_id, now)

        return self.advance(intent_id, now)

    # ------------------------------------------------------------------ steps
    def _apply(self, intent, now):
        prior_state = intent['state']
        intent = self.store.begin_apply(intent['id'], now)
        try:
            job_id = self.partner.apply(
                intent['tenant'], intent['filters'], now, intent_key=intent['intent_key'])
        except PartnerReportFailed:
            raise
        except Exception as error:
            self.store.record_event(intent['id'], now, 'apply_error', {'error': repr(error)})
            raise PartnerUnavailable('apply failed: %s' % error) from error
        recovered = prior_state == 'applying'
        return self.store.attach_job(intent['id'], job_id, now, recovered=recovered)

    def poll(self, intent_id, now):
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'intent': intent}
        if intent['job_id'] is None:
            return {'step': 'needs_apply', 'intent': intent}
        if now >= intent['deadline']:
            return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}

        try:
            remote_state, members = self.partner.status(intent['job_id'], now)
        except PartnerReportFailed as error:
            return {'step': 'failed', 'intent': self.store.mark_failed(intent_id, now, 'partner failed', str(error))}
        except PartnerReportCancelled as error:
            event_id = self.store.record_event(intent_id, now, 'partner_cancelled', {'detail': str(error)})
            decision = self.store.request_cancel(intent_id, now, source='partner')
            return {'step': 'cancelled', 'intent': self.store.get_intent(intent_id),
                    'decision': decision['decision'], 'event_id': event_id}
        except PartnerUnavailable as error:
            self.store.record_event(intent_id, now, 'poll_error', {'error': str(error)})
            return {'step': 'retry', 'intent': self.store.get_intent(intent_id), 'error': str(error)}

        try:
            record = self.store.record_poll(intent_id, now, remote_state)
            if record['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
                return {'step': 'terminal', 'intent': record}
            if remote_state == 'generating':
                return {'step': 'generating', 'intent': record}
            self.store.record_ready(intent_id, members, now)
        except export_store.IntentClosed as error:
            return {'step': 'terminal', 'intent': self.store.get_intent(intent_id)}
        except export_store.DeclarationChanged as error:
            self.store.record_event(intent_id, now, 'declaration_rejected', {'error': str(error)})
            return {'step': 'failed', 'intent': self.store.mark_failed(
                intent_id, now, 'declaration changed between polls', str(error))}
        return self.fetch_links(intent_id, now)

    def poll_ready(self, intent_id, now):
        """Poll once and report the declaration, without downloading or
        refreshing addresses.  This is the step that must not be confused
        with a timeout of the apply call."""
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'intent': intent}
        if intent['job_id'] is None:
            if now >= intent['deadline']:
                return {'step': 'expired',
                        'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}
            intent = self.ensure_submitted(intent_id, now)
        if now >= intent['deadline']:
            return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}
        try:
            remote_state, members = self.partner.status(intent['job_id'], now)
        except PartnerReportFailed as error:
            return {'step': 'failed', 'intent': self.store.mark_failed(intent_id, now, 'partner failed', str(error))}
        except PartnerReportCancelled as error:
            event_id = self.store.record_event(intent_id, now, 'partner_cancelled', {'detail': str(error)})
            decision = self.store.request_cancel(intent_id, now, source='partner')
            return {'step': 'cancelled', 'intent': self.store.get_intent(intent_id),
                    'decision': decision['decision'], 'event_id': event_id}
        except PartnerUnavailable as error:
            self.store.record_event(intent_id, now, 'poll_error', {'error': str(error)})
            return {'step': 'retry', 'intent': self.store.get_intent(intent_id), 'error': str(error)}
        try:
            record = self.store.record_poll(intent_id, now, remote_state)
            if record['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
                return {'step': 'terminal', 'intent': record}
            if remote_state != 'ready':
                return {'step': 'generating', 'intent': record}
            declared = self.store.record_ready(intent_id, members, now)
        except export_store.DeclarationChanged as error:
            self.store.record_event(intent_id, now, 'declaration_rejected', {'error': str(error)})
            return {'step': 'failed', 'intent': self.store.mark_failed(
                intent_id, now, 'declaration changed between polls', str(error))}
        return {'step': 'ready', 'intent': declared, 'members': members}

    def fetch_links(self, intent_id, now, force=False):
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'intent': intent}
        if now >= intent['deadline']:
            return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}
        if not self.store.members(intent_id):
            return {'step': 'needs_ready', 'intent': intent}
        if not force and all(link['expires_at'] is None or link['expires_at'] > now
                             for link in self.store.links(intent_id).values()) \
                and len(self.store.links(intent_id)) == len(self.store.members(intent_id)):
            return {'step': 'links', 'intent': intent, 'links': self.store.links(intent_id), 'refreshed': False}
        try:
            refreshed = self.partner.links(intent['job_id'], now)
        except export_store.LinkConflict as error:
            self.store.record_event(intent_id, now, 'link_conflict', {
                'error': str(error), 'job_id': intent['job_id'],
            })
            return {'step': 'link_conflict', 'intent': intent, 'error': str(error)}
        except (ExportLinksUnavailable, PartnerUnavailable) as error:
            self.store.record_event(intent_id, now, 'links_error', {'error': str(error)})
            return {'step': 'retry', 'intent': intent, 'error': str(error)}
        try:
            self.store.record_links(intent_id, refreshed, now)
        except export_store.IntentClosed:
            return {'step': 'terminal', 'intent': self.store.get_intent(intent_id)}
        except export_store.LinkConflict as error:
            self.store.record_event(intent_id, now, 'link_conflict', {
                'error': str(error), 'job_id': intent['job_id'],
            })
            return {'step': 'link_conflict', 'intent': intent, 'error': str(error)}
        return {'step': 'links', 'intent': self.store.get_intent(intent_id),
                'links': self.store.links(intent_id), 'refreshed': True}

    def is_expired(self, intent_id, now):
        intent = self.store.get_intent(intent_id)
        return intent['state'] == 'expired' or now >= intent['deadline']

    def download_member(self, intent_id, name, now, renew_on_expiry=True, link_url=None):
        """Download and verify one declared member.

        An expired address is renewed for the *same* partner task; if renewal
        returns an address outside this export it is rejected.  A member that
        is not downloadable yet is reported as such (the report notification
        may precede a member being available).  Passing ``link_url`` forces
        the download through that exact (possibly stale) address so the
        renewal path can be demonstrated.
        """
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'outcome': intent['state'], 'intent': intent}
        if now >= intent['deadline']:
            return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}

        if any(member['name'] == name and member['downloaded'] for member in self.store.members(intent_id)):
            return {'step': 'downloaded', 'outcome': 'already_downloaded', 'intent': intent, 'name': name}

        link = self.store.links(intent_id).get(name)
        if link_url is not None:
            link = dict(link or {})
            link['url'] = link_url
            link['job_id'] = link.get('job_id') or _job_from_address(link_url)
        if link is None or (link_url is None and link['expires_at'] is not None and link['expires_at'] <= now):
            if not renew_on_expiry:
                return {'step': 'link_expired', 'intent': intent, 'name': name}
            refreshed = self.fetch_links(intent_id, now, force=True)
            if refreshed['step'] != 'links':
                return refreshed
            link = refreshed['links'].get(name)
            if link is None:
                return {'step': 'retry', 'intent': intent, 'error': 'no address for %s' % name}

        try:
            payload = self.partner.download(link['url'], now)
        except LinkExpired:
            self.store.record_event(intent_id, now, 'link_expired', {
                'name': name, 'job_id': intent['job_id'], 'url': link['url'],
            })
            if not renew_on_expiry:
                return {'step': 'link_expired', 'intent': intent, 'name': name, 'job_id': intent['job_id']}
            refreshed = self.fetch_links(intent_id, now, force=True)
            if refreshed['step'] != 'links':
                return refreshed
            link = refreshed['links'].get(name)
            if link is None:
                return {'step': 'retry', 'intent': intent, 'error': 'no address for %s' % name}
            try:
                payload = self.partner.download(link['url'], now)
            except LinkExpired:
                return {'step': 'link_expired', 'intent': intent, 'name': name}
        except MemberNotReady:
            self.store.record_event(intent_id, now, 'member_not_ready', {'name': name})
            return {'step': 'not_ready', 'intent': intent, 'name': name}
        except PartnerUnavailable as error:
            self.store.record_event(intent_id, now, 'download_error', {'name': name, 'error': str(error)})
            return {'step': 'retry', 'intent': intent, 'name': name, 'error': str(error)}

        if isinstance(payload, dict) and 'job' not in payload and isinstance(link.get('job_id'), str):
            # ExportAPI returns only the part body; the address already
            # belongs to this intent's task, so tag it for provenance checks.
            payload['job'] = link['job_id']

        try:
            result = self.store.record_download(intent_id, name, payload, now)
        except export_store.IntentClosed:
            return {'step': 'terminal', 'outcome': self.store.get_intent(intent_id)['state'],
                    'intent': self.store.get_intent(intent_id), 'name': name}
        except (export_store.RowMismatch, export_store.ColumnMismatch,
                export_store.MemberChanged, export_store.MemberInvalid) as error:
            self.store.record_event(intent_id, now, 'member_rejected', {
                'name': name, 'error': str(error), 'kind': type(error).__name__,
                'job_id': intent['job_id'],
            })
            return {'step': 'invalid', 'intent': self.store.get_intent(intent_id),
                    'name': name, 'error': str(error), 'kind': type(error).__name__}
        if not result['accepted']:
            if result['reason'] == 'already_downloaded':
                # Another worker verified this exact file first.  That is not
                # a conflict: the hash was checked inside the transaction.
                return {'step': 'downloaded', 'outcome': 'already_downloaded',
                        'intent': self.store.get_intent(intent_id), 'name': name}
            return {'step': 'terminal', 'outcome': result['reason'],
                    'intent': self.store.get_intent(intent_id), 'name': name}
        return {'step': 'downloaded', 'outcome': 'accepted', 'intent': self.store.get_intent(intent_id),
                'name': name, 'member': result['member']}

    def publish(self, intent_id, now):
        result = self.store.finalize(intent_id, now)
        if result['outcome'] == 'published' and not result.get('already'):
            self.store.record_event(intent_id, now, 'published_to_downstream', {
                'publication_id': result['publication']['id'],
                'job_id': self.store.get_intent(intent_id)['job_id'],
            })
        return result

    def advance(self, intent_id, now):
        """One externally supplied time step: poll, refresh addresses, download
        whatever is available, and publish if everything is verified."""
        intent = self.store.get_intent(intent_id)
        if intent['state'] in ('published', 'cancelled', 'failed', 'expired', 'superseded'):
            return {'step': 'terminal', 'intent': intent}
        if intent['job_id'] is None:
            if now >= intent['deadline']:
                return {'step': 'expired', 'intent': self.store.mark_expired(intent_id, now, 'local deadline reached')}
            intent = self.ensure_submitted(intent_id, now)

        result = self.poll(intent_id, now)
        if result['step'] not in ('links', 'needs_ready'):
            return result

        pending = [member['name'] for member in self.store.members(intent_id) if not member['downloaded']]
        for name in pending:
            outcome = self.download_member(intent_id, name, now)
            if outcome['step'] in ('terminal', 'expired', 'invalid', 'link_conflict'):
                return outcome
            if outcome['step'] == 'not_ready':
                self.store.record_event(intent_id, now, 'waiting_for_member', {'name': name})
        published = self.publish(intent_id, now)
        return dict(published, step='published' if published['outcome'] == 'published' else 'incomplete')

    def cancel(self, intent_id, now, source='user'):
        """Cancel locally and, best effort, at the partner.

        The local decision is authoritative; the partner call is an audit
        side effect and its failure never makes a cancelled intent publishable.
        """
        intent = self.store.get_intent(intent_id)
        decision = self.store.request_cancel(intent_id, now, source=source)
        if decision['accepted'] and intent['job_id'] is not None:
            try:
                self.partner.cancel(intent['job_id'])
                self.store.record_event(intent_id, now, 'partner_cancel_sent', {'job_id': intent['job_id']})
            except PartnerUnavailable as error:
                self.store.record_event(intent_id, now, 'partner_cancel_failed',
                                        {'job_id': intent['job_id'], 'error': str(error)})
        return decision

    def run_until(self, intent_id, times, cancel_at=None, source='user', actions=None):
        """Advance a plan of virtual times for one intent and return the trace.

        ``cancel_at`` is a virtual time at which the user cancels.  Time is
        processed in order, so a cancel at T is committed before a shard
        processed at T and after one processed before T.  This is how the
        interleaving is made deterministic without sleeping.
        """
        trace = []
        actions = list(actions or [])
        for now in times:
            events = [action for action in actions if action['at'] == now]
            if cancel_at is not None and cancel_at == now:
                events.insert(0, {'kind': 'cancel', 'source': source})
            if not events:
                result = self.advance(intent_id, now)
                trace.append({'at': now, 'step': result['step'],
                              'state': self.store.get_intent(intent_id)['state'],
                              'decision': result.get('decision'),
                              'detail': _trace_detail(result)})
                continue
            for action in events:
                if action['kind'] == 'cancel':
                    decision = self.cancel(intent_id, now, source=action.get('source', 'user'))
                    trace.append({'at': now, 'step': 'cancel', 'accepted': decision['accepted'],
                                  'state': self.store.get_intent(intent_id)['state'],
                                  'decision': decision['decision']})
                elif action['kind'] == 'download':
                    result = self.download_member(intent_id, action['member'], now,
                                                  renew_on_expiry=action.get('renew', True),
                                                  link_url=action.get('link_url'))
                    trace.append({'at': now, 'step': result['step'],
                                  'state': self.store.get_intent(intent_id)['state'],
                                  'detail': _trace_detail(result)})
                elif action['kind'] == 'advance':
                    result = self.advance(intent_id, now)
                    trace.append({'at': now, 'step': result['step'],
                                  'state': self.store.get_intent(intent_id)['state'],
                                  'decision': result.get('decision'),
                                  'detail': _trace_detail(result)})
                elif action['kind'] == 'poll':
                    result = self.poll_ready(intent_id, now)
                    trace.append({'at': now, 'step': result['step'],
                                  'state': self.store.get_intent(intent_id)['state'],
                                  'members': result.get('members'),
                                  'decision': result.get('decision'),
                                  'detail': _trace_detail(result)})
                else:
                    raise ValueError('unknown action %r' % action['kind'])
        return trace


def _trace_detail(result):
    detail = {}
    for key in ('name', 'member', 'error', 'kind', 'missing', 'reason'):
        if key in result and result[key] is not None:
            detail[key] = result[key]
        if 'links' in result:
            detail['links'] = {member: link.get('expires_at') for member, link in result['links'].items()}
    if 'members' in result and result['step'] == 'ready':
        detail['members'] = result['members']
    return detail or None


def summarize_publication(publication):
    """Stable, JSON-friendly summary used by the demo and tests."""
    if publication is None:
        return None
    payload = publication['payload']
    return {
        'publication_id': publication['id'],
        'intent_id': publication['intent_id'],
        'tenant': publication['tenant'],
        'export_job': payload['export_job'],
        'members': payload['members'],
        'rows_total': payload['rows_total'],
        'published_at': publication['published_at'],
        'active': publication['active'],
    }
