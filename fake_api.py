from copy import deepcopy


class PartnerAPI:
    def __init__(self, responses):
        self.responses = deepcopy(responses)
        self.calls = []

    def request(self, credential, tenant, endpoint, now):
        eligible = [response for response in self.responses if response['at'] <= now]
        if not eligible:
            raise ValueError('no response at this virtual time')
        response = deepcopy(eligible[-1])
        self.calls.append({'credential': credential, 'tenant': tenant, 'endpoint': endpoint, 'at': now, 'status': response['status']})
        return response
