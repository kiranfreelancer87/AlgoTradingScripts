import json

with open('data/ACC.json') as af:
    data = json.loads(af.read())
    print(len(data) / 15)
    af.close()
