import time

import json
import requests

mstr = """
using Turing

@model function gdemo(x, y)
  s ~ InverseGamma(2, 3)
  m ~ Normal(0, sqrt(s))
  x ~ Normal(m, sqrt(s))
  y ~ Normal(m, sqrt(s))
end;

function model(_input)
    _model = gdemo(1.5, 2)
    return _model
end
"""


def response_data(resp):
    rdata = resp.json()
    if rdata['status'] != 'ok':
        raise Exception(rdata['message'])
    return rdata["data"]


# BASE_URL = "http://test.withdata.io"
BASE_URL = "http://api.coinfer.ai"
# BASE_URL = "http://0.0.0.0:8081"

session = requests.Session()

# login
res = session.post(
    f"{BASE_URL}/base/login",
    data=json.dumps(
        {
            "username": "admin",
            "password": "admin",
        }
    ),
)
response_data(res)

# create string-model
print("Create a model")
res = session.post(
    f"{BASE_URL}/turing/object",
    data=json.dumps(
        {
            "object_type": "model",
            "name": "S-Model for test",
            "env": "EJULIA_1.8",
            "stype": "string",
            "code": mstr,
        }
    ),
)
rdata = response_data(res)
model_id = rdata['short_id']
print(f"model = {model_id}")

# sample the model
print("Run the model")
input_id = ""
res = session.post(
    f"{BASE_URL}/turing/object",
    data=json.dumps(
        {
            "object_type": "task",
            "model_id": model_id,
            "input_id": input_id,
        }
    ),
)
rdata = response_data(res)
taskid = rdata['short_id']
print(f"taskid = {taskid}")

# check the task status
while True:
    res = session.get(f"{BASE_URL}/turing/object/{taskid}")
    rdata = response_data(res)
    if rdata['status'] not in ['NEW', 'RUN']:
        print(rdata)
        break
    time.sleep(3)
    print("task is still running...")

res = response_data(session.get(f"{BASE_URL}/turing/object/{rdata['output']}"))
