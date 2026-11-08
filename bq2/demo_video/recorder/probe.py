"""Probe: run steps from a JSON file, save screenshots and a DOM summary for each 'shot'."""
import asyncio, json, pathlib, sys
from browser import start, stop, Page, login

SUMMARY = r"""(()=>{const q=s=>[...document.querySelectorAll(s)];
const vis=e=>{const r=e.getBoundingClientRect();return r.width>0&&r.height>0};
return {url:location.href,title:document.title,
 inputs:q('input,textarea,button,select').filter(vis).slice(0,40).map(e=>({tag:e.tagName,id:e.id,name:e.name,type:e.type,ph:e.placeholder,txt:(e.innerText||e.value||'').slice(0,40),cls:(e.className||'').toString().slice(0,60)})),
 links:q('a').filter(vis).slice(0,60).map(a=>[(a.innerText||'').trim().slice(0,40),a.getAttribute('href')]),
 text:document.body?document.body.innerText.slice(0,1500):''}})()"""

async def main(steps_file):
    steps = json.loads(pathlib.Path(steps_file).read_text())
    procs, port = start()
    out = pathlib.Path("/film/probe"); out.mkdir(exist_ok=True)
    try:
        p = await Page.connect(port)
        for s in steps:
            k = s["do"]
            try:
                if k == "login": await login(p)
                elif k == "nav": await p.nav(s["url"], s.get("wait", 2.5))
                elif k == "js": print("js ->", json.dumps(await p.js(s["expr"]))[:600])
                elif k == "wait": await p.wait_for(s["expr"], s.get("timeout", 30))
                elif k == "sleep": await asyncio.sleep(s["s"])
                elif k == "click": await p.click_xy(s["x"], s["y"])
                elif k == "type": await p.type_text(s["text"])
                elif k == "key":
                    MODS = {"alt": 1, "ctrl": 2, "meta": 4, "shift": 8}
                    mods = sum(MODS[m] for m in s.get("mods", "").split("+") if m)
                    key = s["key"]; code = s.get("code") or ({"Enter": "Enter", "Escape": "Escape"}.get(key) or ("Key" + key.upper() if len(key) == 1 else key))
                    vk = s.get("vk") or ({"Enter": 13, "Escape": 27}.get(key) or (ord(key.upper()) if len(key) == 1 else 0))
                    base = {"key": key, "code": code, "windowsVirtualKeyCode": vk, "modifiers": mods}
                    await p.send("Input.dispatchKeyEvent", {"type": "rawKeyDown" if mods else "keyDown", **base})
                    await p.send("Input.dispatchKeyEvent", {"type": "keyUp", **base})
                elif k == "shot":
                    await p.shot(out / f"{s['name']}.png")
                    print("== shot", s["name"]); print(json.dumps(await p.js(SUMMARY), indent=0)[:s.get("max", 2500)])
            except Exception as e:
                print("!! step failed", s, e)
    finally:
        stop(procs)

asyncio.run(main(sys.argv[1]))
