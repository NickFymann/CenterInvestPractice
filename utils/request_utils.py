import requests
from types import SimpleNamespace
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryCallState

BASE_URL = 'http://www.cbr.ru/dataservice'
def log_each_retry(retry_state: RetryCallState):
    attempt = retry_state.attempt_number
    if attempt > 1:
        endpoint = retry_state.args[0]
        params = retry_state.kwargs.get("params", {})
        print(f"[RETRY {attempt}/5] {endpoint} {params}")

@retry(
    stop=stop_after_attempt(5), 
    wait=wait_fixed(15),        
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before=log_each_retry
)
def fetch(endpoint, params=None):
    response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
    response.raise_for_status()
    return response.json(object_hook=lambda d: SimpleNamespace(**d))