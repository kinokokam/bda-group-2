# site

Static dashboard served from `s3://<bucket>/site/` (or opened locally).

- `index.html`: BQ2 exposure charts with Plotly.js, reading `data/*.json` written by `lambdas/publish` (owner: Neeharika).
- `monitor.html`: live monitor with check status per slice and the agent's note, reading `data/monitor/*.json` (owner: Qiaoye).
- Refresh every 60 seconds with a simple `setInterval` + `fetch`.
- If the course account blocks public S3 websites: `aws s3 sync s3://<bucket>/site ./site` and open `index.html` locally.
