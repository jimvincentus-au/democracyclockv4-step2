# Step 2 — code review findings (2026-08-09)

Companion to Cowork's `STEP2_HARVEST_SPEC.md` (Trump Chronicles volume). That
document looks at Step 2 from *downstream* (what the emitted data lacks). This one
looks from *inside the code* (defects in how the pipeline runs). Where they
overlap, this doc confirms or extends Cowork's finding and says so.

**Method.** Read the full backbone (`step2_v4`, `getweekevents`, `buildweekevents`,
`writeweekevents`, `extractor`, `helper`, `builder_helper`, `schema`, `config`),
then four parallel reviewers over all ~48 per-source harvesters/builders, plus a
`py_compile` sweep of every module.

**Confidence labels**, matching Cowork's convention:
- **[self-verified]** — confirmed here directly (compiled, or read the exact lines).
- **[read]** — established by a reviewer reading the code; high confidence.
- **[inference]** — reasoning from structure; needs a runtime check to confirm.

**Severity:** 🔴 critical · 🟠 high · 🟡 medium · 🟢 low / dead code.

---

## Status — fixes landed 2026-08-09

The three highest-priority reliability items are **fixed and verified** (compiled;
behaviorally tested where possible without live API keys):

- ✅ **C-1** — `buildecon` syntax error corrected; whole codebase now compiles.
  `econ` moved to the builder's `OPTIONAL_SOURCES`, so a default run no longer
  builds it from a missing/stale file (still runnable via `--only econ`).
- ✅ **C-3** — Congress harvester now raises on any non-200 (and on 200-with-bad-
  JSON) instead of silently reporting zero bills; missing-key cause logged at
  error level. Verified with a fake-response harness (403 → raise, normal page →
  yields, bad JSON → raise).
- ✅ **H-1** — orchestrator build/write are now best-effort: one failed harvester
  no longer skips the whole week. Per-stage rc plus a new `partial` flag are
  reported so incomplete runs are visible, not blocking.
- ✅ **Bonus** — `step2_v4.py --limit` was silently broken (it passed `--limit` to
  the builder, which only accepts `--limit-per-source`, crashing the build stage).
  Fixed the passthrough; `--limit` now works end-to-end.

Second batch (2026-08-10):

- ✅ **C-2** — Democracy Docket builder now fetches the real article body via
  `extract_events_from_url` (was fabricating empty synthetic text). Verified live:
  a DD news article now yields a real event ("Federal judge dismisses DOJ case for
  D.C. voter roll access", category *Judicial Developments*) where the old code
  emitted one boilerplate row titled "Analysis". **Follow-up filed:** the v5
  sitemap harvester still lists bare section-index pages (`/analysis/`,
  `/opinion/`) as articles and harvests forecast/opinion sections the protocol
  excludes — see "New: C-2b" below.
- ✅ **H-2** — orchestrator now always injects `--skip scotusblog` into get/build/
  write (analysis-only source no longer written to the event log), keeps stripping
  it from `--only`, and still rejects `--only scotusblog`. Verified across default,
  `--only guardian`, and `--only scotusblog` cases.

### New: C-2b (found while fixing C-2) — DD v5 harvester lists non-articles [read + live]
- ✅ **FIXED (2026-08-10)** — `step2_getdemocracydocket_v5.py` now drops URLs whose
  path is a bare section root (`.../analysis/`, `.../opinion/`, `.../news/`, site
  root) via `_is_section_index`; unit-tested across article vs index URLs. Real
  articles (`.../analysis/<slug>/`) are kept.
- ⏳ **Still open (policy decision, not fixed):** the harvester still pulls the
  `/analysis/` and `/opinion/` families, which are largely forecast/opinion the
  Canonical Protocol excludes, so most real DD events come from `/news/`. Whether
  to keep harvesting those two sections is a coverage call for the author/Cowork.

**Live verification (2026-08-09).** Ran the real `step2_v4.py` entry point on
**week 81 (2026-08-01→07)** in the `printify_project` conda env, isolated to a
scratch artifacts dir (production `artifacts/` untouched):
- Full bounded run (`--limit 1`): all **17** default harvesters ran, **0 failures**,
  build + write produced `master_index_week81`; summary `ok:true, partial:false`.
  Confirmed `econ` absent from the default set (C-1) and Congress succeeded with a
  keyed 200 returning 0 bills — a real recess-empty, not a masked error (C-3).
- Forced-failure test (stubbed get returning rc=1): build **and** write still ran;
  summary `ok:false, partial:true` (H-1 proven — old code skipped both).
- Live sightings of still-open findings: `scotusblog` was harvested/built/written
  (H-2); Democracy Docket produced 1 event where Substack posts produced 5–7 (C-2
  signature).

Everything below is the original review; items not marked ✅ are still open.

---

## Part A — reconciliation with Cowork's four findings

Cowork's findings are about the *shape of the emitted data* and are correct on the
Step 2 side. Confirmations and extensions:

- **Cowork F1 (occurrence date never captured).** Confirmed, and **worse than
  "everything defaults to `post_date`."** Several harvesters date events on the
  sitemap **last-modified** timestamp, not even the publication date: Democracy
  Docket ([getdemocracydocket_v5.py:140](step2_getdemocracydocket_v5.py:140)) and
  Free Beacon ([getfreebeacon_v4.py:234](step2_getfreebeacon_v4.py:234)). Daily
  Signal's own docstring
  ([getdailysignal_v4.py:8](step2_getdailysignal_v4.py:8)) explains it deliberately
  avoids `<lastmod>` for exactly this reason — so the sources disagree with each
  other about what "the date" even means. This strengthens Cowork's case for a
  nullable `occurred_on` + always-populated `reported_on` + `dated_by`. **[read]**
- **Cowork F2 (recall with no precision counterweight).** Confirmed at the code
  level: the retry logic ([extractor:121](step2_extractor_v4.py:121)) only ever
  pushes for *more* schema-compliant output; there is no path by which the
  extractor records doubt. Cowork's `extraction_confidence` + `basis`-quote
  proposal has no counterpart in the current code. **[read]**
- **Cowork F3 (no jurisdiction field).** Confirmed — no source writes a
  jurisdiction/federal-nexus field; the writer has no such column
  ([writeweekevents:265](step2_writeweekevents_v4.py:265) `EventRow`). **[read]**
- **Cowork F4 (cross-source identity never established).** Confirmed on the Step 2
  side: `_dedupe_rows` groups strictly by `(source_key, normalized_url)`
  ([writeweekevents:444](step2_writeweekevents_v4.py:444)) — no cross-source
  comparison exists anywhere in Step 2. This was independently flagged in the
  backbone review below (H-2 / M-6). **The `group_id` half of the inference lives
  in Step 3, which is not in this repo** (`step3_prompts_v4.py` is referenced but
  absent here), so the "wrong sources attached to right event" mechanism cannot be
  confirmed from Step 2 alone — Cowork's "verify in the Code session" still needs
  the Step 3 folder. **[read] + [inference unconfirmed]**
- **P2 (suspicious events).** Per Cowork's negative result, **not confirmed, not on
  the fix list.** Nothing here changes that; do not action P2 until it is re-run
  against the archive population.

---

## Part B — code-level defects (new; not in Cowork's spec)

### 🔴 Critical

**C-1. ✅ FIXED — `step2_buildecon_v4.py:318` — syntax error; the module cannot import. [self-verified]**
`if not raw or raw.startswith("(Extraction error:")):` has an unmatched `)`.
`py_compile` fails on this file (and only this file). Because `econ` is in the
default build set ([buildweekevents:36](step2_buildweekevents_v4.py:36)) but its
harvester is commented out ([getweekevents:34](step2_getweekevents_v4.py:34)),
`_load_builder("econ")` catches the import error, logs it, and marks `econ`
failed — so **every default build run returns failure (rc=1)**. The real danger
isn't econ (it produces nothing anyway); it's that operators learn to treat a
red/failed status as normal, which **masks every other failure** (see H-1).

**C-2. ✅ FIXED — Democracy Docket articles reach the LLM effectively empty. [read]**
The registered v5 harvester deliberately does not fetch bodies — it hardcodes
`"summary": ""` and never sets `content`/`html`
([getdemocracydocket_v5.py:160](step2_getdemocracydocket_v5.py:160)) — but the v4
builder only reads `summary` and `content`/`html`
([builddemocracydocket_v4.py:207](step2_builddemocracydocket_v4.py:207)); it never
reads `body_text`, the field the *old* v4 harvester populated. So the model gets
only a slug-derived title + two hardcoded boilerplate lines. A source expected to
yield several events per post silently yields ~0 (or one fabricated echo of the
title), every week. This is the registry version-mismatch, and it is real.

**C-3. ✅ FIXED — Congress harvester returns 0 bills with no key, silently. [self-verified]**
`config_v4.py:35` sets `CONGRESS_API_KEY = None` and no `CONGRESS_GOV_API_KEY`
env var is set; the code only emits a soft `logger.warning`
([getcongress_v4.py:376](step2_getcongress_v4.py:376)) and never checks
`resp.status_code`. On the 403 that Congress.gov returns without a key, the bill
list is `[]`, the pager breaks immediately, and the run "succeeds" with zero
entities. **All congressional events (public laws, vetoes) are silently lost** on
any run without a key. (Confirm whether the production environment sets the env
var — if it does, this is latent rather than active.)

### 🟠 High

**H-1. ✅ FIXED — One flaky harvester aborts the whole build + write. [self-verified]**
Build only runs `if r_get.rc == 0` ([step2_v4.py:236](step2_v4.py:236)), but
`getweekevents` returns rc=1 if *any single* harvester fails
([getweekevents:335](step2_getweekevents_v4.py:335)). With ~17 live-scraping
harvesters, one site 500/timeout/layout-change fails the whole week's build. This
is the archetypal "quietly incomplete for months" failure. Compounded by C-1,
which makes rc=1 the *normal* outcome, so this is invisible. Fix: let the pipeline
proceed with whatever harvested, and report per-source failures distinctly.

**H-2. ✅ FIXED — `scotusblog` is not actually excluded from default runs. [read]**
`_remove_analysis_only_sources` ([step2_v4.py:101](step2_v4.py:101)) only strips
`scotusblog` from an *explicit* `--only`/`--skip`. A plain `--week N` run passes
neither, so scotusblog is harvested, built, and written into the Master Event Log
— contradicting its "analysis-only, excluded" contract. Worse, its category
strings (`Courts / Supreme Court / merits-cases`, etc.) never match the writer's
`Courts / Supreme Court / Blog` bucket
([writeweekevents:27](step2_writeweekevents_v4.py:27)), so every scotusblog event
lands as "unknown" category. An analysis-only source is silently polluting the
event log under mislabeled categories.

**H-3. Both Ballotpedia harvesters silently prefer a stale local HTML file. [read]**
`orders` ([getballotpedia_order_v4.py:88](step2_getballotpedia_order_v4.py:88))
and `shadow` ([getballotpedia_shadow_v4.py:33](step2_getballotpedia_shadow_v4.py:33))
default to a `~/Documents/…Ballotpedia.html` file and, if present, never fetch the
live page. A months-old saved page → new executive orders / shadow-docket
decisions silently missed, no error.

**H-4. Dense Substack sources truncate at 6000 output tokens → later events lost. [read]**
Zeteo, Meidas, 50501, Noah, OutLoud call `extract_events_from_url` without
`max_tokens`, taking the 6000 default ([extractor:508](step2_extractor_v4.py:508)),
while HCR alone passes 9000 ([buildhcr_v4.py:154](step2_buildhcr_v4.py:154)). For a
source whose own prompt expects 20–40 events (Zeteo), output exceeds the cap, the
response is truncated, the single retry runs at the same cap, and events past the
cut are parsed-around and lost with only a compliance warning.

**H-5. `skip_existing` (resume mode) is a no-op in every builder. [read]**
The flag is threaded from CLI/orchestrator but never read
(e.g. [buildguardian_v4.py:212](step2_buildguardian_v4.py:212),
[buildjustsecurity_v5.py:377](step2_buildjustsecurity_v5.py:377)). `--skip-existing`
re-fetches and re-LLMs every source and overwrites prior `eventjson` — wasted
tokens, and prior output lost if the re-run yields fewer events.

**H-6. Right-wing sources have no summary fallback → paywalled articles vanish. [read]**
Examiner/Free Beacon/Daily Signal set `summary: ""` at harvest and their builders
never backfill it (contrast Guardian's `_guardian_fallback_summary`,
[buildguardian_v4.py:161](step2_buildguardian_v4.py:161)). If the article is
paywalled/bot-blocked/JS-rendered, extraction returns empty → event dropped to
`noncompliant` (0 events), and even a parsed event with an empty summary is dropped
again by the writer's `--strict` mode
([writeweekevents:322](step2_writeweekevents_v4.py:322)).

### 🟡 Medium

- **M-1. Congress build drops vetoes/overrides depending on window contents. [read]**
  The keep-filter retains only public laws or salient non-bills; vetoes/pocket-vetoes
  pass neither, and `if kept: items = kept`
  ([buildcongress_v4.py:291](step2_buildcongress_v4.py:291)) applies the filter only
  when it matched something — so a window with ≥1 public law silently discards
  vetoes, but a veto-only window keeps them. A terminal event type included or
  dropped based on unrelated items.
- **M-2. Two divergent canonical parsers, names one underscore apart. [read]**
  `parse_llm_events_canonical` (emits `source_date`,
  [builder_helper:23](step2_builder_helper_v4.py:23)) vs `_parse_llm_events_canonical`
  (emits `date`, [builder_helper:287](step2_builder_helper_v4.py:287)). The writer
  tolerates both today, so **no active data loss**, but only the former recovers a
  missing-`Summary:` block (helper:82) and only the latter tolerates a `===`
  header prefix — if the prompt/model ever emits `===`, the whole Substack family
  parses **zero** blocks. Latent trap; unify them.
- **M-3. Parser hinges on a literal em-dash `—` in the date header. [read]**
  `_HDR_RE`/`_HEADER_RE` require `YYYY-MM-DD — title`
  ([builder_helper:14](step2_builder_helper_v4.py:14); prompt spec at
  [prompts:53](step2_prompts_v4.py:53)). A model emitting a hyphen/en-dash instead
  yields 0 events for that article, recorded noncompliant. Cross-cutting silent
  per-article loss risk — worth a tolerant regex, **especially before switching
  model providers.**
- **M-4. scotusblog builder doesn't force a category. [read]**
  Unlike orders/opinions (which force a default), it trusts the LLM's `Category:`
  line ([buildscotusblog_v4.py:134](step2_buildscotusblog_v4.py:134)); omission →
  empty category → dropped under `--strict`.
- **M-5. Zeteo pagination uses a fixed stride. [read]**
  `offset = page_idx * per` ([getzeteo_v4.py:150](step2_getzeteo_v4.py:150)) — the
  exact bug Bulwark's code documents fixing (advance by `len(posts)`). On windows
  needing backfill past a short page, posts in the skipped offset range are never
  seen. Older windows only.
- **M-6. No cross-source de-duplication.** = Cowork F4, Step 2 side. Same act from
  five outlets → five rows, inflating every count in the Master Event Log. May be
  a deliberate "coverage" choice, but it should be a *decision*, and it directly
  affects any count that backs a book claim.
- **M-7. `ids` selection is 1-based in most builders, 0-based in HCR. [read]**
  Substack family uses `[i-1 for i in ids]`; HCR uses `_pick_indices` (0-based,
  [builder_helper:237](step2_builder_helper_v4.py:237)). `--ids 1 2 3` processes
  different items per source.

### 🟢 Low / dead code

- **G-1. Orphaned harvesters:** `getreuters`, `getpropublica`, `getcbo` define
  `run_harvester` but are in neither registry and have no builders — dead in the
  pipeline (silent coverage gaps if they were meant to be wired). **[read]**
- **G-2. `step2_getjustsecurity_v4 DO NOT USE.py`** confirmed unreferenced — safe
  to delete. **[read]**
- **G-3. `build_fr_presdocs_deterministic_v1.py`** writes the *same* filename as the
  registered FR builder but emits only presidential documents — running it on a
  normal window silently drops Tier-A agency rules/notices. Standalone script; risk
  only if invoked manually. **[read]**
- **G-4. FR builder source tags never applied** — `ev.setdefault("tags", …)` is a
  no-op because the parser always sets `tags: []`
  ([buildfederalregister_v4.py:286](step2_buildfederalregister_v4.py:286)). **[read]**
- **G-5. Migration-relevant staleness in the extractor:** `provider_cap = 16000`
  ([extractor:588](step2_extractor_v4.py:588)) is a gpt-4o-era assumption (that
  model is 128K; Claude is 200K) — only mislabels truncation diagnostics today but
  should be corrected on migration; dead constants `_DEFAULT_EXTRACT_MODEL` /
  `_DEFAULT_TEMPERATURE` ([extractor:34](step2_extractor_v4.py:34)); model
  hardcoded to `gpt-4o` fallback ([extractor:506](step2_extractor_v4.py:506)),
  env-overridable via `OPENAI_MODEL_EVENTS`. **[self-verified]**
- **G-6. Duplicate `setup_logger`** with different behavior in
  [helper:87](step2_helper_v4.py:87) (honors `DC_LOG_POLICY`/`DC_LOG_LEVEL`) vs
  [builder_helper:435](step2_builder_helper_v4.py:435) (ignores them). **[read]**
- **G-7. Cosmetic-but-real:** slug titles via `.capitalize()` lowercase everything
  after the first letter (examiner/freebeacon/dailysignal/DD) — low-quality titles
  fed to the extractor; harvesters build a retry `session` then discard it and use
  bare `requests.get`; `datetime.utcnow()` deprecated
  ([helper:223](step2_helper_v4.py:223)). **[read]**

---

## Part C — implications for the OpenAI → Claude migration

1. **The migration surface is genuinely small.** Every LLM call funnels through
   `call_openai()` ([extractor:349](step2_extractor_v4.py:349)); the schema
   enforcement and retry logic are **plain-text pattern matching**, not
   OpenAI-specific structured-output — so they port cleanly.
2. **M-3 (em-dash dependency) gets riskier at model-swap time.** A new model with
   slightly different punctuation habits could silently zero-out articles. Harden
   the header regex *before or during* the swap.
3. **Cowork F1–F3 are schema additions** (`occurred_on`/`reported_on`/`dated_by`,
   `extraction_confidence`/`basis`, `jurisdiction`/`federal_nexus`). They touch the
   same per-source prompt blocks in `step2_prompts_v4.py` and the extractor's
   output contract. **The migration is the cheapest moment to add them** — you're
   already revising prompts and re-validating output. Do them in the same pass.
4. **Fix G-5 `provider_cap`** to Claude's real context, and set H-4's token caps
   deliberately per source rather than by accident.

---

## Part D — do NOT fix yet

- **P2 (suspicious events).** Cowork could not reproduce it against the pipeline
  population and explicitly says it must be re-run against the *archive* first. Not
  on the fix list.
- **Cowork F4 `group_id` mechanism.** The Step 2 half is confirmed (no cross-source
  identity). The "wrong sources attached to right event" mechanism is in **Step 3**
  and unverifiable from this repo — get the Step 3 folder before acting.

---

## Part E — suggested order

1. **C-1, C-3, H-1 together** — stop the pipeline from silently reporting success
   on empty data and from aborting on one bad harvester. Without this, every other
   fix is hard to verify because "failed" is meaningless.
2. **C-2** — repoint the Democracy Docket builder at a body-bearing harvester (or
   have v5 fetch bodies). A whole source is near-dead.
3. **H-2, H-3, H-6, M-1, M-5** — per-source completeness leaks.
4. **Migration pass** — swap provider (M-3 hardening, G-5), and land Cowork F1–F3
   schema fields in the same prompt revision.
5. **M-2/M-6/H-4/H-5 and the G-items** — consolidation and cleanup.
6. **Then** re-open P2 and the Step 3 `group_id` question with their proper data.
