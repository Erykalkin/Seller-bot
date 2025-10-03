"""
Универсальная функция proxy_request(...) для выполнения запроса через HTTP-прокси.
Зависимости: pip install requests
"""

import requests
import base64
import time
import urllib.parse
from typing import Optional, Tuple, Dict, Any

def proxy_request(
    method: str,
    url: str,
    proxy_hostport: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    use_header_auth: bool = False,
    headers: Optional[Dict[str,str]] = None,
    params: Optional[Dict[str,Any]] = None,
    data: Optional[Any] = None,
    json_data: Optional[Any] = None,
    timeout: Tuple[float,float] = (5.0, 8.0),   # (connect, read)
    allow_redirects: bool = True,
    stream: bool = False,
    verify: bool = True,
    max_retries: int = 0,
    backoff: float = 0.25,
) -> Tuple[bool, Any]:
    """
    Выполнить HTTP request через указанный proxy.

    Возвращает (ok, result):
      - если ok == True: result == requests.Response
      - если ok == False: result == Exception (описание ошибки)
    """
    if headers is None:
        headers = {}
    # создаём session для reuse соединений
    sess = requests.Session()
    sess.trust_env = False  # не подхватывать системные proxy
    sess.headers.update({"User-Agent": "proxy-request/1.0"})
    # пользовательские заголовки перекроют дефолтный UA
    sess.headers.update(headers)

    # формируем proxy_url (URL-escape user/password когда используем их в URL)
    if user and password and not use_header_auth:
        u = urllib.parse.quote(user, safe='')
        p = urllib.parse.quote(password, safe='')
        proxy_url = f"http://{u}:{p}@{proxy_hostport}"
    else:
        proxy_url = f"http://{proxy_hostport}"

    proxies = {"http": proxy_url, "https": proxy_url}

    # если нужно передать креды через заголовок Proxy-Authorization
    if use_header_auth and user and password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        sess.headers["Proxy-Authorization"] = f"Basic {token}"

    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            t0 = time.time()
            resp = sess.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data,
                json=json_data,
                timeout=timeout,
                allow_redirects=allow_redirects,
                stream=stream,
                verify=verify,
                proxies=proxies
            )
            # не обязательно r.raise_for_status(); иногда нужен доступ к телу даже при 4xx/5xx.
            resp.elapsed_seconds = round(time.time() - t0, 3)
            return True, resp
        except Exception as e:
            last_exc = e
            # на ретраях — пауза
            if attempt < max_retries:
                time.sleep(backoff * (1 + attempt))
            else:
                return False, e
    # safety fallback
    return False, last_exc

# ------------------ Примеры использования ------------------

if __name__ == "__main__":
    # Пример 1: простой GET (по plain HTTP endpoint)
    ok, res = proxy_request(
        "GET",
        "http://checkip.amazonaws.com/",
        proxy_hostport="5.161.202.98:10001",        # ТУТ МЕНЯЕМ +1 ДЛЯ КАЖДОГО НОВОГО АККА 
        user="976596d2d843ce431783__cr.ru;anon.1",
        password="7e5fcac4696d2070",
        use_header_auth=False,   # попробуйте True/False в зависимости от прокси
        timeout=(5,8),
        max_retries=1
    )
    if ok:
        print("status:", res.status_code, "elapsed:", getattr(res,"elapsed_seconds",None))
        print("body:", res.text.strip())
    else:
        print("Error:", res)
