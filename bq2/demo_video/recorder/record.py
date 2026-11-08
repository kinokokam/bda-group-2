"""BQ2 demo recorder, built on the demo-video skill's harness (Xvfb framebuffer + ffmpeg x11grab + CDP).
Usage (inside the recorder container):
  python3 record.py TAKE --smoke                     smoke take (Gate 2): first 3 beats, short holds
  python3 record.py TAKE --storyboard /film/sb.md    full take (needs Gate 1 storyboard + Gate 2 smoke)
Writes recordings/TAKE[-smoke].mp4 and a .json sidecar with each beat's ready/end wall times."""
import asyncio, hashlib, json, os, pathlib, signal, subprocess, sys, time
from browser import start, stop, Page, login, W, H, DISPLAY
import beats as B

FPS, CRF = 25, 17
OUT = pathlib.Path("/film/recordings"); OUT.mkdir(exist_ok=True)
args = [a for a in sys.argv[1:]]
SMOKE = "--smoke" in args
SB = args[args.index("--storyboard") + 1] if "--storyboard" in args else None
TAKE = next(a for a in args if not a.startswith("--") and a != SB)
NAME = TAKE + ("-smoke" if SMOKE else "")
BEATS = B.TAKES[TAKE]

if not SMOKE:   # Gates 1 and 2, enforced in code like the skill's harness
    if not SB or not pathlib.Path(SB).exists():
        raise SystemExit("GATE 1: full take needs --storyboard <approved storyboard>")
    if not (OUT / f"{TAKE}-smoke.json").exists() and "--no-smoke" not in args:
        raise SystemExit(f"GATE 2: no smoke take for {TAKE}; run --smoke first and inspect it (or --no-smoke, stamped in the sidecar)")
    if "--no-smoke" in args:
        print("  !! GATE 2 waived (--no-smoke): this take spends a metered resource, so it is filmed once, unrehearsed", flush=True)
else:
    BEATS = [dict(b, hold=min(b.get("hold", 4), 3)) for b in BEATS[:3]]

CINEMA = pathlib.Path("cinema.js").read_text()
OVERLAY = pathlib.Path("overlay.js").read_text()


async def run(port):
    p = await Page.connect(port)
    await login(p)                                   # OFF camera
    for url in {b["nav"] for b in BEATS if b.get("nav", "").startswith(B.AF)}:
        await p.nav(url, 0.5)                        # pre-warm Airflow pages OFF camera (grid bundle into the cache)
    for src in (CINEMA, OVERLAY):
        await p.send("Page.addScriptToEvaluateOnNewDocument", {"source": src})
    await p.nav("about:blank", 0.5)
    # nice: the encoder yields to Chromium when CPU is short (the Docker VM is shared with Airflow + Helical);
    # zerolatency: no look-ahead/B-frames, so x264 never buffers 25+ frames of 1080p RAM.
    ff = subprocess.Popen(["nice", "-n", "10", "ffmpeg", "-y", "-loglevel", "error", "-video_size", f"{W}x{H}", "-framerate", str(FPS),
                           "-f", "x11grab", "-draw_mouse", "0", "-i", f"{DISPLAY}.0", "-c:v", "libx264",
                           "-preset", "ultrafast", "-crf", str(CRF), "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                           str(OUT / f"{NAME}.mp4")], env={**os.environ, "DISPLAY": DISPLAY})
    t_popen = time.time(); time.sleep(1.2)
    if ff.poll() is not None:
        raise SystemExit("ffmpeg exited immediately: nothing recorded")
    marks = []
    try:
        for b in BEATS:
            t_nav = time.time()
            if b.get("nav"):
                await p.nav(b["nav"], b.get("load_wait", 1.0))
            if b.get("ready"):
                ok = await p.wait_for(b["ready"], b.get("ready_timeout", 60))
                if not ok: print(f"  !! {b['label']}: ready condition never true", flush=True)
            await asyncio.sleep(b.get("settle", 0.8))
            for src in (CINEMA, OVERLAY):            # documents opened before injection still get them
                await p.js(src + ";1")
            await p.js(f"__cap({json.dumps(b.get('step',''))},{json.dumps(b.get('cap',''))});1")
            t_ready = time.time()
            for act in b.get("acts", []):
                k = act[0]
                try:
                    if k == "scroll": await p.js(f"__scroll({json.dumps(act[1])},{json.dumps(act[2])},{act[3]})")
                    elif k == "callout": await p.js(f"__cine.callout({json.dumps(act[1])},{json.dumps(act[2])},{{hold:{act[3]}}})")
                    elif k == "spot": await p.js(f"__cine.spotlight({json.dumps(act[1])},{{hold:{act[2]}}})")
                    elif k == "cap": await p.js(f"__cap({json.dumps(act[1])},{json.dumps(act[2])});1")
                    elif k == "sleep": await asyncio.sleep(act[1])
                    elif k == "js": await p.js(act[1])
                    elif k == "click":
                        pt = await p.js("(()=>{const e=document.querySelector(" + json.dumps(act[1]) + ");if(!e)return null;"
                                        "const r=e.getBoundingClientRect();return {x:r.left+r.width/2,y:r.top+r.height/2}})()")
                        if not pt: raise RuntimeError(f"no element {act[1]}")
                        await p.js(f"__cine.cursorTo({pt['x']},{pt['y']}).then(()=>__cine.ripple({pt['x']},{pt['y']}))")
                        await p.click_xy(pt["x"], pt["y"])
                    elif k in ("clickxy", "dblclickxy"):
                        # real CDP mouse events at CSS coords; dblclick = press/release with clickCount 1 then 2
                        x, y = act[1], act[2]
                        await p.js(f"__cine.cursorTo({x},{y}).then(()=>__cine.ripple({x},{y}))")
                        await asyncio.sleep(0.7)
                        await p.send("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
                        for n in ((1,) if k == "clickxy" else (1, 2)):
                            for t in ("mousePressed", "mouseReleased"):
                                await p.send("Input.dispatchMouseEvent", {"type": t, "x": x, "y": y, "button": "left", "clickCount": n})
                    elif k == "wheel":                    # ("wheel", x, y, deltaY) real scroll in the editor
                        await p.send("Input.dispatchMouseEvent", {"type": "mouseWheel", "x": act[1], "y": act[2], "deltaX": 0, "deltaY": act[3]})
                    elif k == "type":                     # ("type", selector_or_None, text)
                        if act[1]: await p.js(f"document.querySelector({json.dumps(act[1])}).focus();1")
                        await p.type_text(act[2])
                    elif k == "key":                      # ("key", "g", "ctrl") or ("key", "Enter")
                        MODS = {"alt": 1, "ctrl": 2, "meta": 4, "shift": 8}
                        key = act[1]; mods = sum(MODS[m] for m in (act[2] if len(act) > 2 else "").split("+") if m)
                        code = {"Enter": "Enter", "Escape": "Escape"}.get(key) or ("Key" + key.upper() if len(key) == 1 else key)
                        vk = {"Enter": 13, "Escape": 27, "ArrowUp": 38, "ArrowDown": 40, "PageUp": 33, "PageDown": 34}.get(key) or (ord(key.upper()) if len(key) == 1 else 0)
                        base = {"key": key, "code": code, "windowsVirtualKeyCode": vk, "modifiers": mods}
                        await p.send("Input.dispatchKeyEvent", {"type": "rawKeyDown" if mods else "keyDown", **base})
                        await p.send("Input.dispatchKeyEvent", {"type": "keyUp", **base})
                    elif k == "ready":
                        if not await p.wait_for(act[1], act[2]): print(f"  !! {b['label']}: {act[1][:60]} not ready", flush=True)
                except Exception as e:
                    print(f"  !! {b['label']} act {k} failed: {e}", flush=True)
            left = b.get("hold", 5) - (time.time() - t_ready)
            if left > 0: await asyncio.sleep(left)
            t_end = time.time()
            marks.append({"label": b["label"], "t_nav": round(t_nav - t_popen, 2),
                          "t_ready": round(t_ready - t_popen, 2), "t_end": round(t_end - t_popen, 2)})
            print(f"  {marks[-1]['t_ready']:7.2f}-{marks[-1]['t_end']:7.2f}  {b['label']}", flush=True)
    finally:
        t_stop = time.time()
        ff.send_signal(signal.SIGINT); ff.wait(timeout=30)
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0",
                                str(OUT / f"{NAME}.mp4")], capture_output=True, text=True).stdout.strip())
    eps = (t_stop - t_popen) - dur          # capture start lag: missing time is at the front
    for m in marks:
        for k in ("t_nav", "t_ready", "t_end"): m[k.replace("t_", "v_")] = round(m[k] - eps, 2)
    side = {"take": NAME, "res": f"{W}x{H}", "fps": FPS, "video_duration": dur, "eps": round(eps, 3),
            "smoke_waived": "--no-smoke" in args, "storyboard": None if SMOKE else {"path": SB, "sha256": hashlib.sha256(pathlib.Path(SB).read_bytes()).hexdigest()[:16]},
            "beats": marks}
    (OUT / f"{NAME}.json").write_text(json.dumps(side, indent=1))
    # Proof frames: one per beat, 1 s after it is ready, for the contact sheet
    fr = OUT / f"{NAME}-frames"; fr.mkdir(exist_ok=True)
    for i, m in enumerate(marks):
        t = min(m["v_ready"] + 1.0, max(0, dur - 0.1))
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{t:.2f}", "-i", str(OUT / f"{NAME}.mp4"),
                        "-frames:v", "1", "-vf", "scale=1280:-1", str(fr / f"{i:02d}_{m['label']}.png")])
    print(f"done: {NAME}.mp4  {dur:.1f}s  eps={eps:.2f}s  frames in {fr.name}/", flush=True)


procs, port = start()
try:
    asyncio.run(run(port))
finally:
    stop(procs)
