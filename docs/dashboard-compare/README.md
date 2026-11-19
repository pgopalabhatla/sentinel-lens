# Dashboard v2: three variants, one spec, one test

Branch `dashboard-v2`, off `dashboard`. Three versions of the live week 2 replay are built from one design spec
and one data export, and tested by one script. This page shows how they compare. It does not pick a winner.

| | Variant | Code | Runs in |
|---|---|---|---|
| A | Streamlit, restyled | [dashboard/a_streamlit/](../../dashboard/a_streamlit/) | `.venv-dashboard` |
| B | One static HTML/JS page, ECharts from a CDN | [dashboard/b_static/](../../dashboard/b_static/) | any local web server |
| C | React + Vite + Recharts | [dashboard/c_react/](../../dashboard/c_react/) | Node (Vite preview) |

The v1 dashboard ([dashboard/app.py](../../dashboard/app.py)) is unchanged. It is the baseline the spec's fixes refer to.

## What is shared

- **One export.** [dashboard/export_ui.py](../../dashboard/export_ui.py) turns the committed demo folder into
  `dashboard/demo/live-week-2/ui/demo.json` (528 KiB) and `series.json` (1.9 MiB). It writes the records with
  their own times and nothing filtered for a clock. It is deterministic: the same bytes under pandas 2.3 and 3.1.
  The export now also carries the change log (`changes.json`, 10 changes). A change counts as visible from its
  stream record, a minute after the change. That is when the triage agent's `recent_changes` could see it.
- **One "nothing from the future" rule.** It is `ui_state(t)` in [dashboard/data.py](../../dashboard/data.py),
  which variant A calls directly. B and C use a line-by-line port,
  [dashboard/shared/visibility.js](../../dashboard/shared/visibility.js).
  [tests/test_dashboard_ui.py](../../tests/test_dashboard_ui.py) runs the port in Node and checks that its output
  equals the Python's at the same 7 clock times: pages, alerts, notes, changes, every page's series points, and
  the attacker view.
- **One look.** [dashboard/shared/theme.css](../../dashboard/shared/theme.css) holds the tokens as CSS
  variables: warm charcoal with a faint chart grid, amber for "needs human", muted teal for healthy, grey for
  unsure. The fonts are IBM Plex Mono and Big Shoulders Display, with one motion: a new card slides in. B and C
  load the file as it is. A injects it, plus [streamlit.css](../../dashboard/a_streamlit/streamlit.css) to fit
  Streamlit's own wrappers to it.
- **One URL and DOM contract**, so one script can drive all three. The URL takes
  `?t=<ISO>&page=<n>&series=<id>&reveal=1&speed=<min>`. The DOM uses the test ids `clock`, `page-count`,
  `page-card[data-page]`, `page-detail`, `verdict`, `note`, `cause`, `impact`, `evidence` and `answer-key`.

## Screenshots

5 fixed clock times, 1440 x 900 (all 33 PNGs are in [a/](a/), [b/](b/), [c/](c/)). Each screenshot shows the
newest page selected and the answer key off. The browser runs in `America/Los_Angeles`, so an axis drawn in
local time would be off by 7-8 h.

| Clock (UTC) | A: Streamlit | B: static + ECharts | C: React + Recharts |
|---|---|---|---|
| Mon 26 Oct 04:30, page 3 just fired (Trino CPU 6.3x, seasonal band) | ![](a/2026-10-26T0430Z_1440x900.png) | ![](b/2026-10-26T0430Z_1440x900.png) | ![](c/2026-10-26T0430Z_1440x900.png) |
| Wed 28 Oct 13:40, page 9 with its follow-up | ![](a/2026-10-28T1340Z_1440x900.png) | ![](b/2026-10-28T1340Z_1440x900.png) | ![](c/2026-10-28T1340Z_1440x900.png) |
| Thu 29 Oct 00:20, pages 11 and 12 at the same minute, 12 merged into 11 | ![](a/2026-10-29T0020Z_1440x900.png) | ![](b/2026-10-29T0020Z_1440x900.png) | ![](c/2026-10-29T0020Z_1440x900.png) |
| Fri 30 Oct 00:30, page 14 (producer crash loop), change marker chg-0007 | ![](a/2026-10-30T0030Z_1440x900.png) | ![](b/2026-10-30T0030Z_1440x900.png) | ![](c/2026-10-30T0030Z_1440x900.png) |
| Sun 01 Nov 01:00, page 17 with its follow-up | ![](a/2026-11-01T0100Z_1440x900.png) | ![](b/2026-11-01T0100Z_1440x900.png) | ![](c/2026-11-01T0100Z_1440x900.png) |
| Mon 26 Oct 04:30, **Reveal answer key** on, scrolled to the list | ![](a/2026-10-26T0430Z_1440x900_reveal.png) | ![](b/2026-10-26T0430Z_1440x900_reveal.png) | ![](c/2026-10-26T0430Z_1440x900_reveal.png) |

1280 x 800, at two of the clocks:

| Clock (UTC) | A | B | C |
|---|---|---|---|
| Mon 26 Oct 04:30 | ![](a/2026-10-26T0430Z_1280x800.png) | ![](b/2026-10-26T0430Z_1280x800.png) | ![](c/2026-10-26T0430Z_1280x800.png) |
| Fri 30 Oct 00:30 | ![](a/2026-10-30T0030Z_1280x800.png) | ![](b/2026-10-30T0030Z_1280x800.png) | ![](c/2026-10-30T0030Z_1280x800.png) |

## Check results

All from [dashboard/e2e/compare.mjs](../../dashboard/e2e/compare.mjs), the same script for each variant
(results: [a/results.json](a/results.json), [b/results.json](b/results.json), [c/results.json](c/results.json)).
Chromium (Playwright 1.65.1), on the same laptop, on Nov 19. The committed results are from the last run, through
`pytest -m ui`. Every check also passed in the run before it.

| Check | A | B | C |
|---|---|---|---|
| No future data in the system panels (8 clocks) | pass | pass | pass |
| Chart x labels UTC 24 h, markers at their times (8 clocks, 41 markers) | pass, worst 0.5 px | pass, worst 0.5 px | pass, worst 0 px |
| Page 14 then page 17: only page 17's notes, no chg-0007 | pass | pass | pass |
| Page 17's evidence starts collapsed after page 14's was opened | pass | pass | pass |
| No truncation: 119 strings shown in full, nothing clipped | pass | pass | pass |
| Answer key off by default; when on, labelled "hidden from the system", all 11 injections | pass | pass | pass |
| Time to first render (median of 3 cold loads; the run before, in brackets) | 894 ms (917) | 1372 ms (994) | 1082 ms (1105) |
| Full play-through, 168 ticks at 1 h / tick (target 400 ms / tick = 67.2 s) | 87.0 s (518 ms / tick) | 67.3 s (400 ms / tick) | 67.3 s (400 ms / tick) |
| Page-outs counted during play: rises steadily, ends at 13 | pass | pass | pass |

What each check does, and what it does not do:

- **Reference.** The expected view comes from `visibility.js`, which pytest checks against `data.py`.
- **No future data.** At each clock the script compares the clock, the page-out count, the list and order of
  page cards, the selected page, its verdict and its note cards with the reference. It then searches the text of
  the timeline and detail panels for three things that did not yet exist: a later note's cause, a later page's
  reason, and a change id not yet streamed. With the toggle off it requires no answer-key panel and no `atk-…`
  id anywhere on the page. It reads text, not pixels. A future point on the chart line is caught only if it
  brings an extra alert marker.
- **Axis and markers.** The script reads the chart's SVG, not the chart library's API. The x labels are the
  `<text>` elements; every one must be `HH:MM`. A line is fitted from label position to UTC time, and its error
  must be at most 3 px. Markers are short vertical strokes, sorted by their computed colour and width: amber
  dashed is an alert, amber 3 px is the page, teal is a change. Each expected marker must have one within 3 px
  of where the labels place its time, and the counts must match. All three variants draw SVG for this reason:
  Vega via `usermeta.embedOptions.renderer`, ECharts' SVG renderer, and Recharts.
- **Page 14 then 17.** The page is loaded at Sun 01 Nov 01:00. The script clicks the card for 14, opens its
  evidence, then clicks the card for 17. It reads the whole detail column, not just the first element. That
  column must hold exactly one page detail, for page 17, with no chg-0007, none of page 14's note texts, all of
  page 17's, and no evidence section already open.
- **Truncation.** At 3 clocks, for the 3 newest pages, the script opens the evidence. It then requires every
  reason, cause, impact and evidence string from the data to appear in full. No such element may have
  `text-overflow: ellipsis`, a line clamp, or clipped overflow. The scrolling columns are allowed to scroll.
- **The checks were shown to fail.** Before the final runs I broke variant B four ways: a local-time axis
  (markers 754 px from their labels), stale detail content (page 17 showing "17+14+17"), ellipsis CSS on the
  note fields, and a 6 h future leak (an extra alert marker). Each was caught. The stale-content check missed the
  first time, because it read only the first `page-detail` element. It now reads the whole column.
- **Timings.** They come from one machine, so they are not benchmarks. B's first render moved from 994 to 1372 ms
  between two runs. First render runs from
  navigation until the clock, detail and chart are drawn and the fonts are loaded. Each load uses a fresh browser
  context, so Google Fonts, and ECharts for B, come over the network every time. Streamlit's server is warmed by
  one throwaway load before timing, so A's number does not include loading the data into Python.

## Size and dependencies

Non-blank lines in each variant's own files. Everything also uses `theme.css` (144) and `export_ui.py` (59).

| | Own files | Lines | Also uses |
|---|---|---|---|
| A | `app.py` 237, `chart.py` 68, `streamlit.css` 66, `.streamlit/config.toml` 22 | **393** | `data.py` (210) |
| B | `index.html` 17, `app.js` 222 (+ `make_single.py` 37 for the one-file build) | **239** (276) | `visibility.js` (111), `format.js` (30) |
| C | `App.jsx` 158, `SeriesChart.jsx` 76, `main.jsx` 7, `index.html` 13, `vite.config.js` 11, `package.json` 20 | **285** | `visibility.js` (111), `format.js` (30) |

The test runner `compare.mjs` is 444 lines, shared by all three. For scale, v1 `app.py` is 175.

| | Installed | Sent to the browser |
|---|---|---|
| A | `streamlit==1.52.2`, `altair==6.0.0` ([requirements-dashboard.txt](../../requirements-dashboard.txt)). They need their own venv: Streamlit pins pandas < 3. `.venv-dashboard` has about 48 packages, 554 MB. | Streamlit's frontend, the fonts |
| B | nothing | ECharts 5.6.0 from jsDelivr (1.0 MB, 336 KB gzipped), the fonts, `demo.json` + `series.json` (2.5 MB); the one-file build is 2.5 MiB |
| C | `react` / `react-dom` 19.3.1 and `recharts` 3.11.0; dev: `vite` 8.3.8 and `@vitejs/plugin-react` 6.3.0. The lockfile has 90 packages (48 dev); `node_modules` is 81 MB. | the built bundle (667 KB JS, 192 KB gzipped; 8 KB CSS), the fonts, the two JSON files |
| tests | `playwright` 1.65.1 (19 MB) plus its Chromium (about 100 MB, in `~/Library/Caches/ms-playwright`) | |

## Found while building

These are the things that cost time, and the place each one is now guarded.

- **A: a keyed container does not remount.** I put the selected page in `st.empty()` inside a container keyed
  by page, and drew it with `st.markdown`. Page 14 then 17 passed on text. But React reused the `<details>`
  elements, so page 14's opened evidence was still open on page 17. `st.html` replaces the block's inner HTML,
  which fixed it. The check above was added because of this.
- **A: a numeric domain on a `utc` scale draws nothing in Streamlit.** The same spec renders correctly in
  standalone Vega. The cause is Streamlit's bundled Vega-Lite. Fix: ISO-string bounds. `tests/test_dashboard_variants.py`
  checks the spec.
- **A: Vega-Lite's `labelFlush` puts the first and last time labels off their ticks.** The axis check flagged
  a 10.9 px error. Fix: `labelFlush=False`.
- **A: Streamlit's `AppTest` cannot rerun a script that has `st.pills` on it.** Its ButtonGroup state fails to
  serialise. The series picker is therefore a horizontal `st.radio` styled as chips.
- **All: every chart library defaults to local time.** Each needed its own fix: Vega `scale.type = "utc"`,
  ECharts `useUTC: true`, and for Recharts a UTC `tickFormatter` plus ticks computed on whole UTC hours. A plain
  temporal axis, as in v1, drew browser-local 12 h labels ("05:30", "06 AM") in a scratch test.
- **Data: JSON has no `Infinity`.** 55 alerts have `z = +inf` (wobble 0). Python writes that, but browsers refuse
  it. `data.load` now turns non-finite numbers into null. The explanation text still says `z=+inf`.
- **Fonts arrive late.** One early screenshot used the fallback font, and the strip wrapped. The runner now
  waits for `document.fonts.ready` before each check and screenshot.

## How to run each

```bash
# once: the UI export (already committed; re-run after changing dashboard/demo/)
python dashboard/export_ui.py

# A: Streamlit (http://127.0.0.1:8601). It must be started from its folder, so it reads .streamlit/config.toml
python3 -m venv .venv-dashboard && .venv-dashboard/bin/pip install -r requirements.txt -r requirements-dashboard.txt
cd dashboard/a_streamlit && ../../.venv-dashboard/bin/streamlit run app.py

# B: any static server rooted at dashboard/ (http://127.0.0.1:8600/b_static/index.html)
cd dashboard && python3 -m http.server 8600 --bind 127.0.0.1
python dashboard/b_static/make_single.py      # one self-contained HTML file in dashboard/b_static/dist/

# C: React + Vite (http://127.0.0.1:8602)
cd dashboard/c_react && npm install && npm run build && npm run preview     # or: npm run dev

# the comparison: starts each server itself, writes docs/dashboard-compare/<variant>/
cd dashboard/e2e && npm install && npx playwright install chromium && node compare.mjs   # or: node compare.mjs b
pytest -m ui                                   # the same, from pytest, and asserts every check passed
```

Every variant takes the same deep link, for example `?t=2026-10-30T00:30Z&page=14&reveal=1`.
