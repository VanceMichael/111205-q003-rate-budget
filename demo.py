import json
from pathlib import Path
from tempfile import TemporaryDirectory
from export_api import ExportAPI
from fake_api import PartnerAPI
from service import eligible


def run():
    fixture = json.loads((Path(__file__).parent / 'fixtures/partner.json').read_text())
    partner = PartnerAPI(fixture['responses'])
    first = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'], 0)
    second = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'], first['retry_after'])
    assert first['status'] == 429 and second['status'] == 200
    assert eligible(0, 5, 6) and not eligible(0, 7, 6)
    export = json.loads((Path(__file__).parent / 'fixtures/exports.json').read_text())
    with TemporaryDirectory() as directory:
        api = ExportAPI(Path(directory) / 'exports.json', export['parts'])
        job = api.apply(export['intent']['tenant'], export['intent']['filters'], 0)
        generating = api.status(job, 4)
        ready = api.status(job, 6)
        member = api.download(api.links(job, 6)[0], 6)
        assert generating['state'] == 'generating' and ready['state'] == 'ready'
    return {'legacy_responses': [first, second], 'export_before_ready': generating, 'export_ready': ready, 'downloaded_member': member}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
