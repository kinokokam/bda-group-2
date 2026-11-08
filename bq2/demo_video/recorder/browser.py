"""Shared plumbing: socat forwards, Xvfb, Chromium with CDP. Used by probe.py and record.py."""
import asyncio, json, os, pathlib, socket, stat, subprocess, time, urllib.request
import websockets

W, H = 1920, 1080          # physical framebuffer, 16:9 = YouTube 1080p
CSS_W, CSS_H = 1280, 720   # CSS viewport at device scale 2
DISPLAY = ":99"
PROFILE = pathlib.Path(os.environ.get("FILM_PROFILE", "/film/profile"))
FORWARDS = [(8080, 8080), (443, 443), (80, 80)]   # container localhost -> host.docker.internal


def start():
    procs = []
    for lp, rp in FORWARDS:
        procs.append(subprocess.Popen(["socat", f"TCP-LISTEN:{lp},fork,reuseaddr,bind=127.0.0.1",
                                       f"TCP:host.docker.internal:{rp}"]))
    procs.append(subprocess.Popen(["python3", "-m", "http.server", "8800", "--bind", "127.0.0.1",
                                   "--directory", "/film/stage"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    x = subprocess.Popen(["Xvfb", DISPLAY, "-screen", "0", f"{W}x{H}x24", "-nolisten", "tcp"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(x)
    sock = pathlib.Path(f"/tmp/.X11-unix/X{DISPLAY[1:]}")
    for _ in range(40):
        if sock.exists() and stat.S_ISSOCK(sock.stat().st_mode):
            break
        time.sleep(0.25)
    else:
        raise SystemExit("no X socket")
    PROFILE.mkdir(parents=True, exist_ok=True)
    for lock in ("DevToolsActivePort", "SingletonLock", "SingletonSocket", "SingletonCookie"):
        (PROFILE / lock).unlink(missing_ok=True)
    (PROFILE / "Default").mkdir(parents=True, exist_ok=True)
    prefs = PROFILE / "Default" / "Preferences"
    if not prefs.exists():
        prefs.write_text(json.dumps({"credentials_enable_service": False,
                                     "profile": {"password_manager_enabled": False}}))
    c = subprocess.Popen(
        ["chromium", "--no-sandbox", "--no-first-run", "--no-default-browser-check",
         "--disable-infobars", "--disable-features=Translate", "--disable-dev-shm-usage",
         "--ignore-certificate-errors", "--disable-smooth-scrolling", "--force-device-scale-factor=1.5", "--force-color-profile=srgb",
         # FILM_CHROME_FLAGS: extra flags for A/B tests (e.g. "--disable-gpu --disable-gpu-compositing"), default none
         *os.environ.get("FILM_CHROME_FLAGS", "").split(),
         "--hide-scrollbars", "--kiosk", "--window-position=0,0", f"--window-size={CSS_W},{CSS_H}",
         "--remote-debugging-port=0", "--remote-allow-origins=*", f"--user-data-dir={PROFILE}",
         "--app=about:blank"],
        env={**os.environ, "DISPLAY": DISPLAY}, stdout=subprocess.DEVNULL, stderr=open("/film/chrome.log", "w"))
    procs.append(c)
    pf = PROFILE / "DevToolsActivePort"
    for _ in range(480):   # up to 120 s: Chromium start-up is slow when the Docker VM is contended ("no CDP port")
        if pf.exists() and pf.read_text().split("\n")[0].strip().isdigit():
            port = int(pf.read_text().split("\n")[0]); break
        time.sleep(0.25)
    else:
        raise SystemExit("no CDP port")
    time.sleep(1.5)
    return procs, port


def stop(procs):
    for p in reversed(procs):
        try: p.terminate()
        except Exception: pass


class Page:
    def __init__(self, ws): self.ws, self._id = ws, 0

    @classmethod
    async def connect(cls, port):
        t = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/json").read())
        page = next(x for x in t if x.get("type") == "page")
        # open_timeout: the default 10 s handshake failed when the Docker VM was CPU-starved by other containers
        ws = await websockets.connect(page["webSocketDebuggerUrl"], max_size=None, open_timeout=90)
        p = cls(ws); await p.send("Page.enable"); await p.send("Runtime.enable")
        return p

    async def send(self, method, params=None, timeout=45):
        self._id += 1; mid = self._id
        await self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                raise RuntimeError(f"CDP timeout on {method}")
            m = json.loads(await asyncio.wait_for(self.ws.recv(), left))
            if m.get("method") == "Page.javascriptDialogOpening":
                print("  (dismissed dialog:", m["params"].get("message", "")[:80], ")", flush=True)
                self._id += 1
                await self.ws.send(json.dumps({"id": self._id, "method": "Page.handleJavaScriptDialog",
                                               "params": {"accept": True}}))
                continue
            if m.get("id") == mid:
                return m

    async def js(self, expr, await_promise=True):
        r = await self.send("Runtime.evaluate", {"expression": expr, "returnByValue": True,
                                                  "awaitPromise": await_promise})
        res = r.get("result", {})
        if "exceptionDetails" in res:
            e = res["exceptionDetails"]
            raise RuntimeError((e.get("exception", {}).get("description") or e.get("text") or "?").splitlines()[0] + " :: " + expr[:120])
        return res.get("result", {}).get("value")

    async def nav(self, url, wait=2.5):
        await self.send("Page.navigate", {"url": url})
        for _ in range(60):
            await asyncio.sleep(0.25)
            try:
                if await self.js("document.readyState") == "complete": break
            except Exception: pass
        await asyncio.sleep(wait)

    async def wait_for(self, expr, timeout=30):
        end = time.time() + timeout
        while time.time() < end:
            try:
                if await self.js(expr): return True
            except Exception: pass
            await asyncio.sleep(0.4)
        return False

    async def shot(self, path):
        import base64
        r = await self.send("Page.captureScreenshot", {"format": "png"})
        pathlib.Path(path).write_bytes(base64.b64decode(r["result"]["data"]))

    async def click_xy(self, x, y):
        for k in ("mouseMoved", "mousePressed", "mouseReleased"):
            await self.send("Input.dispatchMouseEvent", {"type": k, "x": x, "y": y, "button": "left",
                                                         "clickCount": 0 if k == "mouseMoved" else 1})

    async def type_text(self, text):
        for ch in text:
            await self.send("Input.dispatchKeyEvent", {"type": "keyDown", "text": ch, "unmodifiedText": ch, "key": ch})
            await self.send("Input.dispatchKeyEvent", {"type": "keyUp", "key": ch})
            await asyncio.sleep(0.03)


async def login(p):
    """Log in OFF camera (demo-only default credentials of the local stacks)."""
    await p.nav("http://localhost:8080/login/", 1)
    await p.js("const u=document.querySelector('#username'),w=document.querySelector('#password');"
               "if(u){u.value='admin';w.value='admin';w.form.submit()};1")
    await asyncio.sleep(3)
    await p.nav("https://localhost/hi-ee/", 5)
    await p.wait_for("!!document.querySelector('.login-form-admin-action') || !document.title.includes('Login')", 20)
    for attempt in range(3):                      # the login SPA sometimes re-renders mid-sequence
        if not await p.js("!!document.querySelector('.login-form-admin-action')"):
            break                                 # already logged in
        await p.js("document.querySelector('.login-form-admin-action').click();1")
        await p.wait_for("!!document.querySelector('button[type=submit]') && document.querySelector('#username').value", 10)
        await asyncio.sleep(0.8)
        try:
            await p.js("document.querySelector('button[type=submit]').click();1")
        except Exception:
            await asyncio.sleep(2); continue
        if await p.wait_for("!document.title.includes('Login')", 30):
            break
        await p.nav("https://localhost/hi-ee/", 4)
