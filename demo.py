import json
from pathlib import Path
from fake_api import PartnerAPI
from service import eligible


def run():
    fixture = json.loads((Path(__file__).parent / 'fixtures/partner.json').read_text())
    partner = PartnerAPI(fixture['responses'])
    first = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'], 0)
    second = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'], first['retry_after'])
    assert first['status'] == 429 and second['status'] == 200
    assert eligible(0, 5, 6) and not eligible(0, 7, 6)
    return {'responses': [first, second], 'calls': partner.calls}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
