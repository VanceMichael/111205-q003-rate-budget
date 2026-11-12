import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class ExportAPI:
    def __init__(self, path, parts, ready_after=5, link_ttl=3):
        self.path = Path(path)
        self.parts = parts
        self.ready_after = ready_after
        self.link_ttl = link_ttl
        self.jobs = json.loads(self.path.read_text()) if self.path.exists() else {}

    def apply(self, tenant, filters, now):
        job = 'E' + str(len(self.jobs) + 1)
        self.jobs[job] = {'tenant': tenant, 'filters': filters, 'created_at': now, 'cancelled': False}
        self.path.write_text(json.dumps(self.jobs))
        return job

    def status(self, job, now):
        record = self.jobs[job]
        state = 'cancelled' if record['cancelled'] else ('ready' if now >= record['created_at'] + self.ready_after else 'generating')
        return {'job': job, 'state': state, 'members': [{'name': part['name'], 'rows': len(part['rows'])} for part in self.parts] if state == 'ready' else []}

    def links(self, job, now):
        if self.status(job, now)['state'] != 'ready':
            raise ValueError('export is not ready')
        return [f'local://{job}/{part["name"]}?expires={now + self.link_ttl}' for part in self.parts]

    def download(self, link, now):
        parsed = urlsplit(link)
        job = parsed.netloc
        if now >= int(parse_qs(parsed.query)['expires'][0]):
            raise TimeoutError('download link expired')
        if self.status(job, now)['state'] != 'ready':
            raise ValueError('export unavailable')
        index = next(index for index, part in enumerate(self.parts) if part['name'] == parsed.path.lstrip('/'))
        if now < self.jobs[job]['created_at'] + self.ready_after + index:
            raise FileNotFoundError('member not downloadable yet')
        return json.loads(json.dumps(self.parts[index]))

    def cancel(self, job):
        self.jobs[job]['cancelled'] = True
        self.path.write_text(json.dumps(self.jobs))
