# streamlit_bg_app.py
"""
Streamlit app for generating Telegram session strings.

Ключевая правка: гарантированно создаём event loop в текущем потоке
перед импортом pyrogram (pyrogram.sync при импорте вызывает asyncio.get_event_loop()).
"""

# ----------------- VERY IMPORTANT: create event loop BEFORE importing pyrogram -----------------
import asyncio

def ensure_event_loop_for_thread():
    """
    Ensure the current thread has an asyncio event loop set.
    Must be called BEFORE importing pyrogram (because pyrogram.sync
    calls asyncio.get_event_loop() at import time).
    """
    try:
        # If a running loop exists, do nothing.
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop in this thread — create and set one.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# Call immediately, before any pyrogram import.
ensure_event_loop_for_thread()

# ----------------- imports that may require a loop (pyrogram) -----------------
import logging
import threading
import time
import traceback
from concurrent.futures import Future, TimeoutError as FutureTimeout
from typing import Optional
from typing import Optional, Dict
import streamlit as st

# Now safe to import pyrogram
try:
    from pyrogram import Client
    from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired
except Exception as e:
    # If import still fails, give a clear error in UI
    raise RuntimeError(
        "Failed to import pyrogram. Make sure pyrogram is installed and compatible with your Python version."
    ) from e

# Import your DatabaseController
from db_modules.controller import DatabaseController

# ---------------- logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("session_generator")

# ---------------- BackgroundLoop ----------------
class BackgroundLoop:
    """
    Runs an asyncio event loop in a dedicated background thread.
    Use .run(coro) to schedule coroutines there — returns concurrent.futures.Future.
    """
    def __init__(self):
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._started = threading.Event()
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread.start()
        # Wait until the background thread has created its loop
        self._started.wait()

    def _start_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        self._started.set()
        loop.run_forever()

    def run(self, coro: asyncio.coroutines) -> Future:
        if not self.loop:
            raise RuntimeError("Background loop not started")
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self):
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

bg = BackgroundLoop()

# ---------------- helpers ----------------
def session_name_for_phone(phone: str) -> str:
    cleaned = phone.replace("+", "").replace(" ", "").replace("-", "")
    return f"tmp_session_{cleaned}"

# ---------------- async operations (run in bg.loop) ----------------
async def py_send_code_async(api_id: int, api_hash: str, phone: str, proxy: Optional[dict] = None) -> dict:
    session_name = session_name_for_phone(phone)
    client = Client(session_name, api_id=api_id, api_hash=api_hash, proxy=proxy)
    await client.connect()
    try:
        res = await client.send_code(phone)
        phone_code_hash = getattr(res, "phone_code_hash", None) or getattr(res, "phone_hash", None) or res
        sent_at = time.time()
        log.info("SEND_CODE -> phone: %s api_id: %s phone_code_hash: %s sent_at: %s res_repr: %r",
                 phone, api_id, phone_code_hash, sent_at, res)
    finally:
        try:
            await client.stop()
        except Exception:
            log.exception("Error stopping client after send_code")
    return {"phone_code_hash": phone_code_hash, "sent_at": sent_at, "res_repr": repr(res)}


async def py_sign_in_and_export_async(api_id: int, api_hash: str, phone: str, code: str,
                                      phone_code_hash: Optional[str] = None, password: Optional[str] = None, proxy: Optional[dict] = None) -> str:
    session_name = session_name_for_phone(phone)
    client = Client(session_name, api_id=api_id, api_hash=api_hash, proxy=proxy)
    await client.connect()
    try:
        kwargs = {"phone_number": phone, "phone_code": code}
        if phone_code_hash:
            kwargs["phone_code_hash"] = phone_code_hash
        log.info("SIGN_IN -> kwargs: %s time: %s", kwargs, time.time())
        await client.sign_in(**kwargs)
    except SessionPasswordNeeded:
        if not password:
            log.warning("Sign-in requires 2FA password but none provided")
            raise
        await client.check_password(password)
    except (PhoneCodeExpired, PhoneCodeInvalid):
        # propagate so UI can handle specially
        raise
    except Exception:
        log.exception("Unexpected exception during sign_in")
        raise

    try:
        s = await client.export_session_string()
    finally:
        try:
            await client.stop()
        except Exception:
            log.exception("Error stopping client after sign_in/export")

    if not s:
        raise RuntimeError("Failed to export session_string")
    return s


async def save_session_to_db_async(db_url: str, executor_name: str, api_id: int, api_hash: str, phone: str, session_string: str):
    db = DatabaseController(db_url)
    async with db.executors() as repo:
        existing = await repo.get_one_by(name=executor_name)
        if existing:
            if hasattr(repo, "update_param"):
                await repo.update_param(key="name", target=executor_name, column="session_string", value=session_string)
                return {"action": "updated", "name": executor_name}
            else:
                setattr(existing, "session_string", session_string)
                await repo.session.commit()
                return {"action": "updated", "name": executor_name}
        else:
            kwargs = dict(
                name=executor_name,
                api_id=api_id,
                api_hash=api_hash,
                phone=phone,
                session_string=session_string,
            )
            if hasattr(repo, "add_executor"):
                eid = await repo.add_executor(**kwargs)
                return {"action": "created", "id": eid}
            else:
                obj = repo.model(**kwargs)
                repo.session.add(obj)
                await repo.session.commit()
                await repo.session.refresh(obj)
                return {"action": "created", "id": getattr(obj, "id", None)}


async def load_executors_async(db_url: str):
    db = DatabaseController(db_url)
    async with db.executors() as repo:
        rows = await repo.get_all()
        return rows


async def fetch_proxy_from_db(db_url: str, executor_name: str) -> Optional[Dict]:
    """
    Вернёт прокси-словарь или None.
    Ожидается, что репозиторий имеет метод get_one_by(name=...).
    """
    db = DatabaseController(db_url)
    async with db.executors() as repo:
        row = await repo.get_one_by(name=executor_name)
        if not row:
            return None

        # Подстраховка: проверяем наличие IP/host
        ip = getattr(row, "proxy_ip", None) or getattr(row, "host", None)
        if not ip:
            return None

        # Составляем словарь в формате, который pyrogram понимает
        try:
            port = int(getattr(row, "proxy_port", 0) or 0)
        except Exception:
            port = 0

        proxy = {
            "scheme": getattr(row, "proxy_type", "socks5"),
            "hostname": ip,
            "port": port,
            "username": getattr(row, "proxy_user", None),
            "password": getattr(row, "proxy_pass", None),
        }

        # Если порт некорректен или отсутствует — считаем прокси невалидным
        if not proxy["port"]:
            return None

        return proxy


# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="Session generator (bg loop)", layout="centered")
st.title("Session string generator (Background loop)")

st.markdown("""
Заполни форму, нажми **Send code**. После получения кода введи его и нажми **Verify & Save**.
Кнопка Verify блокируется до успешной отправки кода — это предотвращает race condition.
""")

# Inputs
db_url = st.text_input("DB URL (SQLAlchemy style)", "sqlite+aiosqlite:///data/app.db")
executor_name = st.text_input("Executor name", "")
api_id_in = st.text_input("api_id", "")
api_hash_in = st.text_input("api_hash", "")
phone = st.text_input("phone (+7...)", "")

# session_state defaults
st.session_state.setdefault("pending", {})
st.session_state.setdefault("phone_code_hash", None)
st.session_state.setdefault("phone_code_sent_at", None)
st.session_state.setdefault("sending", False)
st.session_state.setdefault("can_verify", False)

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    send_code_btn = st.button("Send code")
with col2:
    verify_btn = st.button("Verify & Save")
with col3:
    clear_btn = st.button("Clear")

if clear_btn:
    st.session_state.pending = {}
    st.session_state.phone_code_hash = None
    st.session_state.phone_code_sent_at = None
    st.session_state.sending = False
    st.session_state.can_verify = False
    st.success("Cleared")

# ---- Send code action ----
if send_code_btn:
    if st.session_state.sending:
        st.warning("Отправка кода уже в процессе, подождите.")
    elif not (executor_name and api_id_in and api_hash_in and phone and db_url):
        st.error("Fill executor_name, api_id, api_hash, phone, db_url")
    else:
        try:
            api_id = int(api_id_in)
            api_hash = api_hash_in.strip()
        except Exception:
            st.error("api_id must be integer")
            api_id = None

        if api_id:
            st.info("Sending code...")
            st.session_state.sending = True
            st.session_state.can_verify = False

            # get proxy from DB for this executor (synchronously via bg.run)
            proxy_dict = None
            try:
                fut_proxy = bg.run(fetch_proxy_from_db(db_url, executor_name))
                proxy_dict = fut_proxy.result(timeout=10)
                log.info("Fetched proxy for send_code: %s", proxy_dict)
            except Exception:
                log.exception("Failed to fetch proxy from DB (will try without proxy)")
                proxy_dict = None

            # call send_code with proxy
            fut = bg.run(py_send_code_async(api_id, api_hash, phone, proxy=proxy_dict))
            res = None
            try:
                res = fut.result(timeout=30)
            except FutureTimeout:
                st.error("Timeout while sending code")
            except Exception as e:
                st.exception(e)
            finally:
                st.session_state.sending = False

            if res:
                st.session_state.phone_code_hash = res.get("phone_code_hash")
                st.session_state.phone_code_sent_at = res.get("sent_at")
                st.session_state.pending = {
                    "executor_name": executor_name,
                    "api_id": api_id,
                    "api_hash": api_hash,
                    "phone": phone,
                    "db_url": db_url,
                }
                st.session_state.can_verify = True
                st.session_state.last_used_proxy = proxy_dict  # save proxy for reuse during Verify
                st.success(f"Code sent at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(res.get('sent_at')))}. Введите код и нажмите Verify & Save.")
                log.info("Saved phone_code_hash to session_state: %s", st.session_state.phone_code_hash)


# ---- Verify & Save action (form) ----
if st.session_state.pending:
    st.markdown("### Введите код и пароль (если нужен)")
    if st.session_state.sending:
        st.warning("Отправка кода всё ещё в процессе — дождитесь её завершения.")
    if not st.session_state.can_verify:
        st.info("Кнопка Verify будет доступна после успешной отправки кода.")
    with st.form("verify_form"):
        code = st.text_input("Code", key="form_code")
        password = st.text_input("2FA password (optional)", type="password", key="form_pass")
        submit = st.form_submit_button("Verify & Save")
        if submit:
            if not st.session_state.get("phone_code_hash"):
                st.error("Нет phone_code_hash — нажмите Send code и дождитесь ответа сервера.")
            elif not st.session_state.get("can_verify", False):
                st.warning("Код ещё не готов к верификации. Нажмите Send code и дождитесь подтверждения.")
            else:
                pending = st.session_state.pending
                phone_code_hash = st.session_state.phone_code_hash
                st.info(f"Verifying and exporting session_string... (using phone_code_hash={phone_code_hash})")

                # try to reuse last used proxy, otherwise fetch again
                proxy_dict = st.session_state.get("last_used_proxy")
                if proxy_dict is None:
                    try:
                        fut_proxy = bg.run(fetch_proxy_from_db(pending["db_url"], pending["executor_name"]))
                        proxy_dict = fut_proxy.result(timeout=10)
                        log.info("Fetched proxy for sign_in: %s", proxy_dict)
                    except Exception:
                        log.exception("Failed to fetch proxy for sign_in (will try without proxy)")
                        proxy_dict = None

                fut = bg.run(py_sign_in_and_export_async(
                    pending["api_id"], pending["api_hash"], pending["phone"],
                    code, phone_code_hash, password, proxy=proxy_dict
                ))

                try:
                    session_string = fut.result(timeout=60)
                except PhoneCodeExpired:
                    st.error("Код устарел (PHONE_CODE_EXPIRED). Нажмите Send code ещё раз.")
                    session_string = None
                except PhoneCodeInvalid:
                    st.error("Неверный код (PHONE_CODE_INVALID). Попробуйте ещё раз.")
                    session_string = None
                except SessionPasswordNeeded:
                    st.error("Требуется двухфакторный пароль. Введите его и попробуйте снова.")
                    session_string = None
                except FutureTimeout:
                    st.error("Timeout при ожидании результата sign_in.")
                    session_string = None
                except Exception:
                    st.error("Ошибка при sign_in — смотрите трассировку ниже.")
                    st.text(traceback.format_exc())
                    session_string = None

else:
    st.info("Нажмите Send code, чтобы начать процесс генерации session string.")

# ---- Load executors preview ----
if st.button("Load executors from DB"):
    if not db_url:
        st.error("Provide db_url")
    else:
        fut = bg.run(load_executors_async(db_url))
        try:
            rows = fut.result(timeout=30)
        except FutureTimeout:
            st.error("Timeout while loading executors")
            rows = []
        except Exception as e:
            st.exception(e)
            rows = []
        st.write(rows)

st.caption("Session strings are sensitive — store them safely.")
