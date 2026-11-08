# Helical Instant BI chat: UI flow for the recorder

Mapped 8 Nov 2026 without any Bedrock call (verified: `hi_llm_usage_audit` still 15 rows, and
the page's fetch/XHR log showed only `ai/list-charts`, which has no LLM behind it).
All coordinates are CSS px in the 1280x720 viewport of `browser.py` (screenshots are 1920x1080, so
multiply by 1.5 to read them against a PNG). Source of truth for selectors: Helical's bundle
`/Users/qiaoye/helical/helicalinsight-7.0.0-docker/hi/hi-ee/js/main.chunk.js` plus live DOM dumps
(`/film/probe/chat_p2.log`, `chat_p3.log`, `chat_p4.log`).

## Cost rules (read first)

Bedrock is called by exactly three frontend actions. Each is ONE Instant action, nothing else:

| Action | Request | LLM | Notes |
|---|---|---|---|
| Connecting a model (double-click on a `.model` row in the picker) | `ai/recommendation/domain` then `ai/recommendation/analyst` | yes, 1 call (`/analyst`, ~8.4k in) | fires every time a model is connected, including a reconnect. Function `dO` in the bundle. |
| Sending a question (Enter or send button) | `ai/interactive-chat` | yes, 1 question (3-4 Haiku calls, ~26-36k in) | one audit row per question |
| "Explain this chart" fab on an answer (`[data-testid=data-insight-play-btn]`, class `chart-preview-section__data-insight-fab`) | `ai/data-insight` | yes | never click it |

Everything else is free: opening the picker, browsing folders, Escape, the folder icon "Open Instant (Edit)",
`ai/list-charts` (static chart list), `ai/load-chat` (re-runs stored SQL, no LLM), the chart-type tabs on an answer,
`ai/convert-chart` (no LLM, says so in `convert_chart.py`).

Budget: 16 of 20 questions used. The final take therefore allows ONE connect (1 call) and the question(s) you film.
Do not reconnect the model after the take and do not re-run the take without checking the budget.
There is no saved chat to reopen: `hi_resource_instant` is empty, and the folder icon's dialog shows "No data".

## Page

- URL after `login`: `https://localhost/hi-ee/#/instant-bi`. Right after `login()` the SPA may bounce to
  `#/admin/overview`; set `location.href` to the URL and then `wait_for("!!document.querySelector('#ib-search-input')")`
  (fall back to `location.hash='#/instant-bi'` and wait again). Document title becomes `HI: Instant`.
- `beforeunload` is armed as soon as a chat exists: navigating away shows "Leave site?" (browser.py auto-accepts dialogs).
- Landing screenshot: `/film/probe/chat_01_landing.png` (centered welcome, empty chat box, cube with blue dot).

## Selectors and coordinates (landing state, nothing connected)

Top bar (height 40):

| Element | Selector | Rect [x,y,w,h] | Click point |
|---|---|---|---|
| Home icon | `.navbar-left-item .anticon-home` | [2,12,21,16] | (12,20) |
| "Instant" module badge (blue pill) | `svg[data-testid=hi-instant-bi-icon]` + text "Instant" | [69,11,64,17] | (100,20) |
| Tab bar | `.hi-navbar-middle .ant-tabs.ant-tabs-editable-card` | [220,0,118,40] | |
| Active tab "Untitled 1" | `.ant-tabs-tab.ant-tabs-tab-active .ant-tabs-tab-btn` | [220,19,70,20] | (255,29) |
| Tab close (x) | `.ant-tabs-tab-active .ant-tabs-tab-remove` | [290,22,24,14] | (302,29) |
| Add tab (+) | `button.ant-tabs-nav-add[aria-label="Add tab"]` (visible one) | [320,18,18,21] | (329,28) |
| Save (dropdown: Save / Save As) | `li.hi-navbar-right-item-dropdown .anticon-save` | [903,12,28,18] | (917,21) |
| Settings (no-op) | `li.hi-navbar-right-item .anticon-setting` | [941,12,18,18] | (950,21) |
| Open Instant (Edit) = folder icon | `li.hi-navbar-right-item .anticon-folder-open` | [969,12,18,18] | (978,21) |
| Connect Semantic Model (cube, same picker as the chat-box cube) | `li.hi-navbar-right-item .cube-icon` | [997,12,16,18] | (1005,21) |
| Share | `li.hi-navbar-right-item .hi-navbar-share-icon` | [1023,12,18,18] | (1032,21) |
| Bulb / Help / Bell / user "HI hiadmin" | `li.user-action` x4 | [1062,11],[1099,11],[1136,11],[1173,0,97,40] | |

The taskbar items carry no stable ids; the tooltips (antd) only exist while hovered. Use the icon class
or the LI order inside `.hi-navbar-right-item` (save, settings, folder, cube, share).

Chat box (`div.ib-chat-container` inside `div.instant-bi-chat-container.instant-bi-chat-container--centered`):

| Element | Selector | Rect | Click point |
|---|---|---|---|
| Welcome heading | `h2` "Welcome to Instant BI" | [538,291,204,33] | |
| Sub heading | `h3` "Connect your Semantic Model to enable smart, chat-based insights tailored to your needs." | [314,335,652,26] | |
| Question textarea | `textarea#ib-search-input.search-input` (placeholder "Ask anything about your data here...") | [206,377,869,23] | (640,388) |
| Model/cube picker button | `button.ib-metadata-button[data-testid=hi-instant-bi-agent-icon]` | [206,416,34,34] | (223,433) |
| Blue "no model" dot | `.ib-metadata-not-present-dot` (child of the cube button) | [230,418,8,8] | |
| Send button | `button.send-button[data-testid=ib-chat-send-button]` (`.anticon-send`) | [1040,416,34,34] | (1057,433) |
| Abort button (replaces Send while the bot works) | `button.stop-button[data-testid=ib-chat-abort-button]` | same place as Send | |
| Beat loader "Bot is Typing" (while working, above the box; click aborts) | `.instant-bi-beat-loader` | | |
| AI disclaimer line (appears once messages exist) | `p[data-testid=ib-ai-disclaimer]` | | |
| Suggested questions block (after connect) | `.chat-suggestions`, header label "Suggested", items `.suggestion-item .suggestion-item__title` (3 shown); clicking one SENDS it (costs a question) | | |

Once a model is connected: the dot disappears, the cube icon gets `span.instant-bi-metadata-selected`, the
textarea tooltip reads "Chatting with: AML BQ2 analysis" (hover only), and the welcome headings make way for
the message list (`.instant-bi-message-list`). The chat container drops `--centered` and the chat box moves
to the bottom (`.instant-bi-footer-container`); re-measure `#ib-search-input` with `getBoundingClientRect()`
before typing. Cube click when a model is already connected does nothing (`O||g()`).

## Model picker (File Browser)

Clicking the cube opens a full-screen antd modal: `.ant-modal.fileBrowser-drawer` [0,0,1280,720]
(screenshot of the root listing: `/film/probe/chat_03_picker_folder.png`, table only, title "File Browser").

| Element | Selector | Rect | Click point |
|---|---|---|---|
| Title | `h4.filebrowser-header` "File Browser" | [25,22,...] | |
| Search / menu / new-folder / refresh icons | `.anticon-search` [1110,26], `.anticon-menu` [1142,28], `.anticon-folder-add` [1175,26], `#hi-file-browser-refresh` [1207,26] | | |
| Close (X) | `.fileBrowser-drawer .anticon-close[aria-label=close]` (the `.ant-modal-close` button is 0x0) | [1239,26,16,16] | (1247,34); Escape also closes |
| Header row | `thead th` Name / Path / Last Modified / Extension / Permission Level | [25,74,1230,42] | |
| Data rows | `tbody tr.ant-table-row` ; row n (1-based) top = 115 + 42*(n-1), height 42; Name cell x 25..271 | | centre y = 136 + 42*(n-1) |
| Root row 1 | folder `aml_bq2` (`.anticon-folder` icon, `span.ant-typography` text) | [25,115,1230,42] | (148,136) |

The list renders a skeleton first (`.skeleton-container`); wait for `tr.ant-table-row` before clicking.
It is an antd tree table with `expandRowByClick: true` (bundle, `onRow` near "expand-parent"): a SINGLE click
on a folder row expands it in place, its children appear as nested rows `tr.ant-table-row-level-1` directly
under it (no breadcrumb, no page change), and a second click collapses it again. So a double-click on a folder
(probe chat_p4) expands and collapses: the row only turns light blue / selected (`expand-parent` class).
Files react to `onDoubleClick` only: for a `.model` file in the picker that is the connect callback.
JS-dispatched `dblclick` events do nothing (React needs the real pointer sequence); use CDP
`Input.dispatchMouseEvent` pressed/released with clickCount 1 then 2 for the model row.

Inside `aml_bq2` (see `/film/probe/chat_03_picker_aml_bq2.png` and the "aml_bq2 listing" section below):
the model row is "AML BQ2 analysis" (file `AML_BQ2_analysis.model`, extension `model`). Double-clicking it
CONNECTS the model (function `dO` -> `instantbi/instant/getAgent`, green toast "Semantic model connected
successfully.", then the `/analyst` Bedrock call and the "Suggested" skeleton -> three suggestions).
A single click on the row only selects it (no network). Nothing in this mapping clicked that row.

## Open saved chat (folder icon, top right)

`li.hi-navbar-right-item .anticon-folder-open` at (978,21) opens the same File Browser filtered to
Instant files; with the current repository it shows "No data" (`/film/probe/chat_05_open_dialog.png`).
No past answers can be shown without spending a question. If a chat is saved later (Save icon at (917,21) ->
"Save", pick folder `aml_bq2`), reopening it goes through `instantbi/instant/getReportForEdit` and per-message
`ai/load-chat` (SQL only), with no `/analyst` call, so that would be a free way to show answers in a retake.
Each loaded message shows `.ib-load-chat-loading-bar` / "Just a moment..." until its SQL has run.

## Step order for the take

(a) Connect the model (1 Bedrock call)
1. `login`; set `location.href='https://localhost/hi-ee/#/instant-bi'`; wait for `#ib-search-input`; settle 3 s.
2. Click (223,433) (cube). Wait `!!document.querySelector('.ant-modal.fileBrowser-drawer tr.ant-table-row')`.
3. Click ONCE at (148,136) (row `aml_bq2`; a double-click would collapse it again). Wait for the nested rows:
   `!!document.querySelector('.ant-modal.fileBrowser-drawer tr.ant-table-row-level-1')`.
4. Double-click the model row `AML BQ2 analysis` at (148,178) (CDP pressed/released clickCount 1 then 2, ~80 ms apart). Wait for connect:
   `!document.querySelector('.ant-modal.fileBrowser-drawer') && !!document.querySelector('.instant-bi-metadata-selected')`.
   The suggestions then load (`.chat-suggestions .ant-skeleton` -> `.suggestion-item`, about 5-10 s); the
   recommendation call can be aborted via the x in `.chat-suggestions__loading-bar`, but the Bedrock call is already made.
   Optional "connected" screenshot once `document.querySelectorAll('.suggestion-item').length>=1 || !document.querySelector('.chat-suggestions__loading-bar')`.

(b) Type the question
5. Re-measure the textarea: `JSON.stringify(document.querySelector('#ib-search-input').getBoundingClientRect())`; click its centre.
6. Type with real key events (`type_text`): the textarea is React-controlled (`onChange` -> Redux `CHANGE_INPUT_VALUE`);
   setting `.value` from JS is ignored. Avoid a literal Enter in the text (Enter sends; Shift+Enter / Ctrl+Enter add a newline).
   Suggested question (known correct, handover table): "Which payment format has the highest laundering rate?"
   (expected ACH 3,828 of 599,685 = 0.64%) or "How many transfers were there on each day?" (bar chart, 8 days).

(c) Send
7. Press Enter (key `Enter`, windowsVirtualKeyCode 13) or click (send button centre, re-measured; landing position is (1057,433)).
   Immediately after: textarea `disabled`, Send swapped for `[data-testid=ib-chat-abort-button]`, `.instant-bi-beat-loader`
   with "Bot is Typing" above the box, the user bubble `.message-container__chat-message-user` in `.instant-bi-message-list`.
   Sending without a model does nothing but a frontend warning toast ("Please select/connect a semantic model before sending message."), no network.

(d) Detect the answer
- Still loading: `!!document.querySelector('.instant-bi-beat-loader') || !!document.querySelector('[data-testid=ib-chat-abort-button]') || document.querySelector('#ib-search-input').disabled`
- Ready (the backend answers with one JSON response, no streaming, so everything appears at once; charts are Muze, SVG/canvas):

```js
(()=>{const bot=[...document.querySelectorAll('.message-container__bot-content')];
 const last=bot[bot.length-1]; if(!last) return false;
 const idle=!document.querySelector('.instant-bi-beat-loader')&&!document.querySelector('[data-testid=ib-chat-abort-button]')&&!document.querySelector('#ib-search-input').disabled;
 const viz=last.querySelector('.ib-live-preview svg, .ib-live-preview canvas, .chart-container svg, .chart-container canvas, .ib-data-table table, .ib-data-table');
 const err=last.querySelector('.ib-response-error, .message-container__chat-message--error');
 return idle&&(!!viz||!!err)})()
```

  Typical latency 25-40 s per question (handover timestamps). Poll with `wait_for(..., timeout=120)`. Give the Muze
  chart another 1-2 s after `ready` for its draw animation before the hero screenshot.
- Error states to recognise on camera: `.ib-response-error` ("Something went wrong in vf_template"),
  `.message-container__chat-message--error`, text "Response aborted" or "Unable to load response". A transient Bedrock
  `ServiceUnavailableException` shows as an error bubble and still costs the attempt (handover question 6).

Answer layout (from the handover and the bundle): a markdown paragraph (`.message-container__bot-content > *` via
react-markdown, generic narrative), then `.chart-container` with an icon-tab strip (`.icon-tabs-container .icon-tab-btn`,
preview / table / other views, plus maximise) and the chart in `.chart-preview-section .ib-live-preview`; tables render as
`.ib-data-table` with pagination; numbers are unrounded (0.6383351259411191). The generated SQL is not shown anywhere
and token usage only appears for data-insight (never used). The "Explain this chart" fab sits at the top right of the
preview: do not click it.

## Screenshots

- `/film/probe/chat_01_landing.png`: Instant BI landing, empty chat box, cube with blue dot, tab "Untitled 1".
- `/film/probe/chat_03_picker_folder.png`: File Browser root (folder `aml_bq2`). `chat_02_picker_root.png` is the same state.
- `/film/probe/chat_03_picker_aml_bq2.png`: inside `aml_bq2`, model row visible (not clicked).
- `/film/probe/chat_05_open_dialog.png`: "Open Instant (Edit)" dialog, "No data".
- `/film/probe/chat_04_back.png`, `chat_06_final.png`: landing again after Escape (dot still present, nothing connected).

## aml_bq2 listing (probe 5, `/film/probe/chat_p5.log`, screenshot `chat_03_picker_aml_bq2.png`)

After one click on the folder row (icon turns to `folder-open`, row highlighted light blue):

| Row | Selector | Rect | Cells (Name / Path / Last Modified / Extension / Permission) | Click point |
|---|---|---|---|---|
| 1 folder `aml_bq2` | `tr.ant-table-row-level-0.expand-parent` | [25,115,1230,42] | aml_bq2 / aml_bq2 / Thu Nov 5 2026 1:49 pm / - / Read + Write | (148,136) toggles expand |
| 2 model `AML BQ2 analysis` | `tr.ant-table-row-level-1` (icon `.cube-icon`) | [25,157,1230,42]; Name cell [25,157,246,42] | AML BQ2 analysis / aml_bq2/AML_BQ2_analysis.model / Thu Nov 5 2026 5:32 pm / model / Read + Write + Delete + Share | (148,178): double-click = CONNECT (1 Bedrock call) |

Only one file exists in the folder on the picker side (the picker lists models; the dashboards are
`efwdd` files and do not show here). Robust selector for the model row:
`[...document.querySelectorAll('.ant-modal.fileBrowser-drawer tr.ant-table-row-level-1')].find(t=>/AML BQ2 analysis/.test(t.innerText))`
and take its `getBoundingClientRect()` centre instead of the fixed (148,178) if the folder list ever grows.
