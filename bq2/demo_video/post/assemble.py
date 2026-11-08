#!/usr/bin/env python3
"""Post-production for the BQ2 demo video: trims, joins and encodes the takes written by recorder/record.py.

Usage:
  python3 post/assemble.py                                   # takes setup run chat close -> post/final.mp4
  python3 post/assemble.py --takes setup-smoke run-smoke --out post/test_final.mp4
  python3 post/assemble.py --cut hard --no-sheet --chapter-min 0

For every take NAME it reads recordings/NAME.mp4 and recordings/NAME.json (the sidecar with per-beat
`v_ready`/`v_end` video-time marks). A missing take is skipped with a warning.

What it does
  1. Trims each take to [first v_ready - PRE_ROLL, last v_end]: blank/login frames before the first beat never appear.
  2. Inside a take, a gap > GAP_MAX between one beat's v_end and the next beat's v_ready (page loading) is cut
     down to GAP_KEEP, joined by a 0.3 s crossfade (--cut xfade, default) or a hard cut (--cut hard).
  3. Takes are joined with a 0.4 s dip to colour (#14574a): fade out at the end of a take, fade in at the start
     of the next. Everything is normalised to 1920x1080, 25 fps, yuv420p.
  4. Encodes OUT with libx264 -crf 18 -preset slow, +faststart, plus a silent AAC track (anullsrc) so every
     player and YouTube behave.
  5. Writes <out dir>/chapters.txt (YouTube "MM:SS Title" lines, titles from beats.py `step`/`cap`),
     <out stem>.json (every beat's final-timeline position) and <out dir>/contact.png (one frame per beat,
     6 columns, in chapter order).
Stdlib + ffmpeg/ffprobe only.
"""
import argparse
import json
import math
import pathlib
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
FILM = HERE.parent
sys.path.insert(0, str(FILM / "recorder"))
try:
    import beats as B  # noqa: E402  (ARCH URL etc. irrelevant; we only read step/cap)
except Exception as e:  # pragma: no cover
    B = None
    print(f"warning: could not import recorder/beats.py ({e}); chapter titles fall back to beat labels", file=sys.stderr)

W, H, FPS = 1920, 1080, 25
PRE_ROLL = 0.3      # seconds kept before the first beat's v_ready
GAP_MAX = 3.0       # a v_end -> next v_ready gap longer than this is dead time
GAP_KEEP = 0.8      # ...and is shortened to this (split evenly around the cut)
XFADE = 0.3         # crossfade length at an internal cut
DIP = 0.4           # fade-to-colour length between takes
DIP_COLOUR = "0x14574a"
CRF, PRESET = 18, "slow"

# Titles for the beats whose caption is empty (chapter cards).
CARD_TITLES = {"OPEN": "Part 1: set up", "PART2": "Part 2: run the pipeline", "RECAP": "Recap"}


def q(t):
    """Quantise a time to the 25 fps frame grid so trims and xfade offsets agree."""
    return round(round(t * FPS) / FPS, 3)


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if r.returncode:
        sys.stderr.write(r.stderr[-4000:])
        raise SystemExit(f"command failed ({r.returncode}): {' '.join(cmd[:6])} ...")
    return r


def probe_duration(path):
    r = run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)])
    return float(r.stdout.strip())


def beat_meta():
    """label -> (step, cap) from beats.py."""
    meta = {}
    if B is None:
        return meta
    for take in B.TAKES.values():
        for b in take:
            meta[b["label"]] = (b.get("step", ""), b.get("cap", ""))
    return meta


def chapter_title(label, meta):
    step, cap = meta.get(label, ("", ""))
    if not cap:
        return CARD_TITLES.get(label, label.replace("_", " ").title())
    short = cap
    if ":" in cap and cap.index(":") <= 45:          # "Architecture: one S3 bucket..." -> "Architecture"
        short = cap.split(":", 1)[0]
    else:
        for stop in (". ", "...", ", "):
            if stop in short:
                short = short.split(stop, 1)[0]
                break
    short = short.rstrip(".").strip()
    if len(short) > 70:
        short = short[:67].rstrip() + "..."
    return f"{step}: {short}" if step else short


def plan_take(name, side, cut):
    """Return the segment list for one take: [{"s","e","xfade"}] in source video time, plus beat positions."""
    beats = side["beats"]
    dur = float(side.get("video_duration") or probe_duration(FILM / "recordings" / f"{name}.mp4"))
    segs = []
    start = q(max(0.0, beats[0]["v_ready"] - PRE_ROLL))
    for i, b in enumerate(beats):
        nxt = beats[i + 1] if i + 1 < len(beats) else None
        if nxt and nxt["v_ready"] - b["v_end"] > GAP_MAX:
            segs.append({"s": start, "e": q(b["v_end"] + GAP_KEEP / 2), "xfade": cut == "xfade"})
            start = q(nxt["v_ready"] - GAP_KEEP / 2)
    segs.append({"s": start, "e": q(min(beats[-1]["v_end"], dur)), "xfade": False})
    # Final-timeline offset of every segment inside the take.
    off = 0.0
    for k, sg in enumerate(segs):
        if k and segs[k - 1]["xfade"]:
            off -= XFADE
        sg["off"] = off
        off += sg["e"] - sg["s"]
    take_dur = off

    def to_take_time(t):
        for sg in segs:
            if sg["s"] - 1e-6 <= t <= sg["e"] + 1e-6:
                return sg["off"] + (t - sg["s"])
        # t fell inside a removed gap: snap to the nearest segment edge
        best = min(segs, key=lambda sg: min(abs(t - sg["s"]), abs(t - sg["e"])))
        return best["off"] + (0 if abs(t - best["s"]) < abs(t - best["e"]) else best["e"] - best["s"])

    marks = [{"label": b["label"], "ready": to_take_time(b["v_ready"]), "end": to_take_time(min(b["v_end"], dur))}
             for b in beats]
    return segs, marks, take_dur


def build_graph(takes, cut):
    """ffmpeg inputs + filter_complex. One -ss/-to input per segment (fast seek), per-take xfade chain, dips, concat."""
    inputs, filt, idx = [], [], 0
    norm = f"setpts=PTS-STARTPTS,fps={FPS},scale={W}:{H}:flags=bicubic,format=yuv420p,setsar=1"
    take_labels = []
    for ti, tk in enumerate(takes):
        seg_labels = []
        for sg in tk["segs"]:
            inputs += ["-ss", f"{sg['s']:.3f}", "-to", f"{sg['e']:.3f}", "-i", str(tk["mp4"])]
            filt.append(f"[{idx}:v]{norm}[s{idx}]")
            seg_labels.append((f"s{idx}", sg))
            idx += 1
        # chain the segments of this take
        cur, cur_dur = seg_labels[0][0], seg_labels[0][1]["e"] - seg_labels[0][1]["s"]
        for k in range(1, len(seg_labels)):
            lab, sg = seg_labels[k]
            d = sg["e"] - sg["s"]
            out = f"t{ti}c{k}"
            if seg_labels[k - 1][1]["xfade"]:
                filt.append(f"[{cur}][{lab}]xfade=transition=fade:duration={XFADE}:offset={cur_dur - XFADE:.3f}[{out}]")
                cur_dur += d - XFADE
            else:
                filt.append(f"[{cur}][{lab}]concat=n=2:v=1:a=0[{out}]")
                cur_dur += d
            cur = out
        fades = []
        if ti > 0:
            fades.append(f"fade=t=in:st=0:d={DIP}:c={DIP_COLOUR}")
        if ti < len(takes) - 1:
            fades.append(f"fade=t=out:st={cur_dur - DIP:.3f}:d={DIP}:c={DIP_COLOUR}")
        out = f"T{ti}"
        filt.append(f"[{cur}]{','.join(fades) if fades else 'null'}[{out}]")
        take_labels.append(out)
    if len(take_labels) > 1:
        filt.append("".join(f"[{l}]" for l in take_labels) + f"concat=n={len(take_labels)}:v=1:a=0[vout]")
    else:
        filt.append(f"[{take_labels[0]}]null[vout]")
    return inputs, ";".join(filt)


def mmss(t):
    t = int(math.floor(t + 1e-6))
    return f"{t // 3600:d}:{t % 3600 // 60:02d}:{t % 60:02d}" if t >= 3600 else f"{t // 60:02d}:{t % 60:02d}"


def first_frame_mean(path):
    """Mean grey level (0-255) of the first frame; ~255 means a blank white page."""
    r = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-frames:v", "1", "-vf", "scale=192:108",
                        "-pix_fmt", "gray", "-f", "rawvideo", "-"], capture_output=True)
    if r.returncode or not r.stdout:
        return None
    return sum(r.stdout) / len(r.stdout)


def contact_sheet(final, marks, out_png, cols=6):
    n = len(marks)
    if not n:
        return
    rows = math.ceil(n / cols)
    with tempfile.TemporaryDirectory(dir=HERE) as td:
        td = pathlib.Path(td)
        total = probe_duration(final)
        for i, m in enumerate(marks):
            # 1 s after the caption appears, but never past the beat's own end (or the film's)
            t = min(m["final_ready"] + 1.0, m["final_end"] - 0.2, total - 0.1)
            t = max(t, m["final_ready"], 0.0)
            run(["ffmpeg", "-y", "-v", "error", "-ss", f"{t:.3f}", "-i", str(final), "-frames:v", "1",
                 "-vf", "scale=480:270", str(td / f"f{i:03d}.png")])
        run(["ffmpeg", "-y", "-v", "error", "-framerate", "1", "-i", str(td / "f%03d.png"), "-frames:v", "1",
             "-vf", f"tile={cols}x{rows}:padding=6:margin=6:color=0x14574a", str(out_png)])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--takes", nargs="+", default=["setup", "run", "chat", "close"], help="ordered take names")
    ap.add_argument("--out", default=str(HERE / "final.mp4"), help="output mp4 (chapters/contact/json go beside it)")
    ap.add_argument("--recordings", default=str(FILM / "recordings"))
    ap.add_argument("--cut", choices=["xfade", "hard"], default="xfade", help="transition at an internal dead-time cut")
    ap.add_argument("--chapter-min", type=float, default=10.0,
                    help="drop chapters that start < N s after the previous kept one (YouTube needs >= 10 s); 0 keeps all")
    ap.add_argument("--no-sheet", action="store_true", help="skip the contact sheet")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and the ffmpeg command, encode nothing")
    a = ap.parse_args()

    rec = pathlib.Path(a.recordings)
    out = pathlib.Path(a.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    meta = beat_meta()

    takes = []
    for name in a.takes:
        mp4, js = rec / f"{name}.mp4", rec / f"{name}.json"
        if not mp4.exists() or not js.exists():
            print(f"warning: take '{name}' skipped (missing {'mp4' if not mp4.exists() else 'json'} in {rec})", file=sys.stderr)
            continue
        side = json.loads(js.read_text())
        if not side.get("beats"):
            print(f"warning: take '{name}' skipped (no beats in sidecar)", file=sys.stderr)
            continue
        segs, marks, tdur = plan_take(name, side, a.cut)
        takes.append({"name": name, "mp4": mp4, "segs": segs, "marks": marks, "dur": tdur})
    if not takes:
        raise SystemExit("no usable takes; nothing to assemble")

    # Final timeline
    off, all_marks = 0.0, []
    for tk in takes:
        tk["off"] = off
        for m in tk["marks"]:
            all_marks.append({"take": tk["name"], "label": m["label"], "final_ready": off + m["ready"],
                              "final_end": off + m["end"], "title": chapter_title(m["label"], meta)})
        off += tk["dur"]
    planned = off

    print("Plan:")
    for tk in takes:
        cuts = sum(1 for s in tk["segs"][:-1])
        print(f"  {tk['name']:<14} {len(tk['segs'])} segment(s), {cuts} dead-time cut(s), {tk['dur']:.2f}s, starts at {mmss(tk['off'])}")
        for sg in tk["segs"]:
            print(f"      keep {sg['s']:8.2f} - {sg['e']:8.2f}  ({sg['e'] - sg['s']:.2f}s){'  then xfade' if sg['xfade'] else ''}")

    inputs, graph = build_graph(takes, a.cut)
    n_video_inputs = sum(len(t["segs"]) for t in takes)       # the anullsrc input comes right after them
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-stats", *inputs,
           "-f", "lavfi", "-t", f"{planned + 1:.3f}", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
           "-filter_complex", graph + f";[{n_video_inputs}:a]atrim=duration={planned:.3f},asetpts=PTS-STARTPTS[aout]",
           "-map", "[vout]", "-map", "[aout]",
           "-c:v", "libx264", "-crf", str(CRF), "-preset", PRESET, "-pix_fmt", "yuv420p", "-r", str(FPS),
           "-c:a", "aac", "-b:a", "64k", "-movflags", "+faststart", "-shortest", str(out)]
    if a.dry_run:
        print("\nffmpeg command:\n  " + " ".join(cmd))
        return

    print(f"\nEncoding {out.name} (planned {planned:.2f}s)...", flush=True)
    r = subprocess.run(cmd, text=True, capture_output=True)
    if r.returncode:
        sys.stderr.write(r.stderr[-6000:])
        raise SystemExit("ffmpeg failed")
    dur = probe_duration(out)
    print(f"Final duration: {dur:.2f}s ({mmss(dur)}); planned {planned:.2f}s, drift {dur - planned:+.2f}s")

    mean = first_frame_mean(out)
    if mean is not None:
        verdict = "BLANK?" if mean > 235 else "ok"
        print(f"First-frame mean grey: {mean:.0f}/255 -> {verdict}")
        if verdict != "ok":
            print("warning: the first frame looks blank; check the first beat's v_ready in the sidecar", file=sys.stderr)

    # Chapters
    lines, last = [], None
    for m in all_marks:
        t = 0.0 if not lines else m["final_ready"]           # YouTube needs the first chapter at 00:00
        if last is not None and a.chapter_min and t - last < a.chapter_min:
            m["chapter"] = False
            continue
        m["chapter"] = True
        lines.append(f"{mmss(t)} {m['title']}")
        last = t
    chapters = out.parent / "chapters.txt"
    chapters.write_text("\n".join(lines) + "\n")
    print(f"\nBeats -> final timeline ({len(lines)} chapters written to {chapters}):")
    for m in all_marks:
        flag = " " if m["chapter"] else "-"
        print(f" {flag} {mmss(m['final_ready'])}  {m['final_ready']:7.2f}s  {m['label']:<12} {m['title']}")
    if any(not m["chapter"] for m in all_marks):
        print(f"   ('-' = folded into the previous chapter: starts < {a.chapter_min:g}s after it; --chapter-min 0 keeps all)")
    print("\n" + "\n".join(lines))

    side = {"out": str(out), "duration": dur, "planned": planned, "cut": a.cut,
            "takes": [{"name": t["name"], "offset": t["off"], "duration": t["dur"], "segments": t["segs"]} for t in takes],
            "beats": all_marks}
    out.with_suffix(".json").write_text(json.dumps(side, indent=1))

    if not a.no_sheet:
        sheet = out.parent / "contact.png"
        contact_sheet(out, all_marks, sheet)
        print(f"\nContact sheet: {sheet} ({len(all_marks)} frames, 6 columns, reading order = beat order above)")


if __name__ == "__main__":
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        raise SystemExit("ffmpeg and ffprobe must be on PATH")
    main()
