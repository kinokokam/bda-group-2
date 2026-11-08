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

## Code shots are the real VS Code

File views are filmed in VS Code's web build (openvscode-server), with the repo mounted read-only, on a
private Docker network so nothing is published on the host:

    docker network create bq2film
    docker run -d --name bq2-vscode --network bq2film -v "$PWD":/home/workspace/bda-group-2:ro \
        gitpod/openvscode-server:latest --without-connection-token --host 0.0.0.0 --disable-workspace-trust
    docker cp recorder/vscode_settings.json bq2-vscode:/home/workspace/.openvscode-server/data/User/settings.json
    docker restart bq2-vscode

`vscode_settings.json` hides `.env`, `.venv`, caches and logs from the explorer, sets a light theme and a
17 px font, and turns off the welcome page, sticky scroll and folding. Close the Copilot chat pane once per
recorder profile (Ctrl+Alt+B; it persists). `beats.vsc(label, path, step, caption, hold, line=N)` opens a file by
URL payload and jumps to a line with Ctrl+G, which also highlights it. The `docker-compose.yml` shot is
positioned so the default Postgres/Fernet lines (11-12) stay off screen.
