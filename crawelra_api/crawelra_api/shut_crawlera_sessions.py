import requests
import base64
from w3lib.http import basic_auth_header
import json
from authentification import *

url = 'http://proxy.crawlera.com:8010'
headers = {
    "Authorization": basic_auth_header(api_key, '')
}
r = requests.get("http://proxy.crawlera.com:8010/sessions/", headers=headers)
all_req = json.loads(r.text)

for key, val in all_req.items():
    requests.delete("http://proxy.crawlera.com:8010/sessions/" + key, headers=headers)
