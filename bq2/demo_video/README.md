# BQ2 demo video: how it was made (8 Nov 2026)

The video follows the `demo-video` skill (ooiyeefei/ccc): every frame is the real product, filmed by a
browser in a Linux container, with captions drawn over it. No voice, no music. Storyboard (Gate 1):
`../docs/demo_video_storyboard.md`. Output: `~/Desktop/bda submission/BQ2_demo_v1.mp4` (7:27, 1080p) and
`chapters.txt` (paste into the YouTube description).

## Re-making it

1. Airflow (`docker compose up -d` at the repo root) and Helical (`~/helical/helicalinsight-7.0.0-docker`) must be up.
   Give Docker Desktop at least 8 GB (the Airflow scheduler alone takes 2.8 GB when starved).
2. `python genstage.py` builds the stage pages (real repo files + captured CLI output) into `stage/`.
   Edit `FILM` and `REPO` paths at the top; captured text goes in `captures/`.
3. `docker build -t bq2-recorder recorder/` once. Then, per take:
   `FILM_PROFILE=/film/profile-main recorder/run.sh record.py setup --smoke` (inspect the frames),
   then `... record.py setup --storyboard /film/storyboard.md`; same for `run`, `chat`, `close`.
   `chat` spends Bedrock calls: 1 to connect the model + 1 per question (`CHAT_QUESTIONS`, default 2).
4. `python post/assemble.py --takes setup run chat close --out post/final.mp4` trims page loads, joins the
   takes with a dip to green, adds a silent audio track and writes `chapters.txt` + `contact.png`.

Notes learned the hard way: Airflow 2.8 renders its grid inside a shadow root, so "page ready" checks
must search shadow roots (`beats.NODES`); React Flow only draws visible task nodes; `.env`/`.env.example`
are shown as placeholders only; the recorder never holds AWS credentials.
