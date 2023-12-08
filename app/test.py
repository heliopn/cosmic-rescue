import requests
def api_gateway_request(url_endpoint="https://qfrricxfo7.execute-api.us-east-2.amazonaws.com/Prod/passenger"):
    url = url_endpoint

    # Change the phrase
    body = {"passengerid": "0170_01"}

    resp = requests.post(url, json=body)

    print(f"status code: {resp.status_code}")
    print(f"text: {resp.text}")

api_gateway_request()