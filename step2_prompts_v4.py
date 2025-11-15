# prompts_v4.py
"""
Prompt prefaces + canonical protocol for builders.

Usage patterns:
- Builders call get_prompt_preface(source_key) to fetch the brief source-specific preface.
- Builders then usually compose: preface + "\n\n" + CANONICAL_EXTRACTION_PROTOCOL
  via compose_system_prompt(source_key).
- For judicial (e.g., SCOTUS shadow docket) channels, prepend JUDICIAL_PREAMBLE
  before the canonical rules in the extractor.

Keep prefaces short and factual. Put ALL formatting/dating/output rules in the
canonical protocol to avoid conflicts.
"""

from __future__ import annotations
from typing import Dict

# ---------------------------------------------------------------------------
# Canonical rulebook (single source of truth for output format & rules)
# ---------------------------------------------------------------------------

CANONICAL_EXTRACTION_PROTOCOL = """
Canonical Extraction Protocol (v1.1 — Final Hardened Version)

PURPOSE:
Identify and record every democracy-affecting event described in the provided text.
Follow this protocol exactly. Do not improvise, reformat, or combine steps.
Your output must be line-by-line compliant. Any deviation is a failure.

DEFINITION — WHAT COUNTS AS AN “EVENT”:
A single, concrete act that has occurred or been officially announced (past-tense facts only).
Examples include:
• executive orders, agency directives, regulations, or enforcement actions
• court rulings, filings, indictments, or stays
• legislative votes, bills, hearings, investigations, or subpoenas
• elections, campaign filings, appointments, or removals
• statements or leaks revealing new facts of legal, political, or ethical consequence
• protests, censorship, arrests, sanctions, or military deployments
Not events: analysis, speculation, opinions, or reactions.

SCOPE AND EXHAUSTIVENESS:
• Extract every democracy-relevant act; do not merge distinct acts.
• If the input lists many discrete acts, output them all.
• Some sources may instruct “exactly one event” in the preface — honor that.

DATING RULE:
• Use `post_date` (YYYY-MM-DD) unless the text clearly provides a newer, specific action date.
• Skip events older than 14 days unless explicitly newly relevant.

OUTPUT FORMAT — FOLLOW THIS EXACT SCHEMA FOR EACH EVENT:
1) Header (single line, must use an EM DASH “—”):
   {YYYY-MM-DD} — {Concise factual event title}

2) Labeled fields (each on its own line, in this order, no blank lines between):
   Summary: {one short, neutral paragraph (≈60–110 words) — who did what, where, outcome/next step, sufficient
      detail for determining the affect the event has on democracy}
   Source: {one or more DIRECT URLs, space-separated, NO labels, NO punctuation, NO dashes}
   Category: {choose exactly one domain from the list below}
   Why Relevant: {one crisp sentence explaining the democratic significance}
   attacks: [{comma-separated handles or empty}]  ← always present, even if empty

3) Footer (mandatory):
   Total events found: [#]
   [END OF LOG]

STRICT FORMAT RULES:
• Plain text only. No JSON, no code fences, no bullets, no headings, no emojis.
• Use the literal EM DASH (—) in the header (not a hyphen).
• Do not add extra blank lines or extra fields.
• “Summary” = what happened. “Why Relevant” = why it matters. Keep them separate.
• “attacks:” must appear for every event; use `attacks: []` when no category applies.

CATEGORY OPTIONS (pick exactly one per event — policy domain, not process):
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence

SELF-CHECK BEFORE OUTPUT:
✅ Header line uses EM DASH and correct date format.
✅ “Source:” contains only direct URL(s), space-separated (no labels like “Ballotpedia — …”).
✅ Category is one of the 12 above.
✅ Every event has an `attacks:` line (either with one or more handles or `[]`).
✅ Footer includes Total events found + [END OF LOG].
✅ If the preface says “exactly one event,” then Total events found: [1].

ZERO-FABRICATION RULE:
Only report actions explicitly described in the text. Do not infer or invent.
If no qualifying events exist, output nothing.
""".strip()

# =======================================================================

PREFACE_SHADOW: str = """
SOURCE: U.S. Supreme Court — Shadow Docket / Emergency Orders (plus parallel emergency rulings from lower federal courts).
TYPE: Single-action judicial decisions that immediately alter what government may do.
STYLE: Procedural but decisive docket-level acts — stays, injunctions, denials, vacaturs, or remands — often brief yet with major institutional impact.
AUDIENCE: Readers tracking how emergency judicial decisions shift power, rights, or election administration.

DATA INPUTS:
• Each record is short and structured (CASE, DECISION_DATE, SOURCE_ROW, brief docket text).
• Each represents exactly ONE operative order — this source is ONE-ACT-PER-RECORD.
• Treat these as official judicial ACTS, not media stories or commentary.

TEXT CHARACTERISTICS:
• Every entry corresponds to a single court action such as “Application denied,” “Stay granted,” or “Order vacated.”
• Background context is minimal; your task is to describe posture and effect only.
• Use strictly neutral, factual language — what changed and for whom.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: emit exactly one event per record.
2. Selection: describe only the operative judicial act (stay, injunction, denial, vacatur, remand, administrative stay, etc.).
3. Granularity: do not merge separate orders or include non-operative commentary.
4. Neutrality: report the posture and consequence; never infer motives or merits.
5. Completeness: summarize who acted, what changed, and the immediate legal or policy consequence.

CATEGORY DISCIPLINE:
Assign the event to the substantive policy domain most directly affected by the order:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
If multiple domains could apply, choose the most immediate substantive one (e.g. an order altering election rules → “Elections & Representation,” not “Judicial Developments”).

DATE RULE:
• Prefer the decision or operative order date.
• If unavailable, default to the provided `post_date`.

SOURCE LINE:
• Attribute as: `Source: Supreme Court Shadow Docket — {case name}` 
  or use the most specific docket/PDF link available.

OUTPUT HANDOFF:
• After applying this Shadow-Docket-specific guidance, follow the ATTACKS FIELD instructions and the Canonical Extraction Protocol that follow this preface.
• Do not restate output schema or footer rules here — the canonical block defines the authoritative format for labels, order, and inclusion of the `attacks: [...]` line.
""".strip()

# =======================================================================

# ---------------------------------------------------------------------------
# Substack default preface (fallback for Substack builders without a custom one)
# ---------------------------------------------------------------------------

SUBSTACK_DEFAULT_PREFACE: str = """
CONTEXT:
This input represents a Substack post plus fetched article text. Expect narrative
recaps or essays that may mention multiple government actions. Your job is to
extract only the concrete acts (filings, rulings, executive/legislative moves)
per the Canonical protocol. Use `post_date` for dating unless told otherwise.
""".strip()

# ---------------------------------------------------------------------------
# Per-source Substack prefaces (short, factual context envelopes)
# ---------------------------------------------------------------------------

PREFACE_MEIDAS: str = """
SOURCE: MeidasTouch (Substack) — “Today in Politics” bulletins.
TYPE: Daily and weekly political roundups summarizing current U.S. political, legal, and governmental developments.
STYLE: Multi-item news briefs or bullet lists written in short, factual paragraphs with minimal context or commentary.
AUDIENCE: Readers seeking rapid, pro-democracy coverage of the day’s concrete governmental and political actions.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• The full article body is fetched for extraction; ignore section headers, images, and embedded media.
• Each bulletin may list 8–20 separate acts, announcements, rulings, or filings.

TEXT CHARACTERISTICS:
• Items are brief and typically factual — e.g., “The court issued…,” “The House passed…,” “The DOJ filed…”.
• Commentary or framing often appears around factual sentences — ignore this and extract only the verifiable acts.
• Each discrete act becomes one event; there are usually many per issue.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract every democracy-relevant act — one event per act. If the bulletin reports 15 acts, expect ≈15 events.
2. Selection: include new, verifiable actions, rulings, orders, filings, appointments, sanctions, protests, hearings, or votes.  
   Exclude speculation, partisan opinion, or forecasts of future acts.
3. Granularity: if one paragraph reports several actions (“The Senate confirmed X and the President signed Y”), record both separately.
4. Completeness: typical yield is 10–25 event blocks depending on bulletin length and density.
5. Neutrality: past tense, factual tone; no adjectives or evaluative phrasing.
6. Independence: treat each bulletin independently. If an act reappears later, record it again if substantively updated.

CATEGORY DISCIPLINE:
Assign each event to one of the 12 canonical Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
If multiple domains could apply, select the most immediate substantive one.

DATE RULE:
• Default to the bulletin’s `post_date`.
• If an item specifies a newer explicit date (“On Friday…,” “On 10/29…”), use that date for that event.
• Ignore dates that refer only to background or past context.

SOURCE LINE:
• Attribute as: `Source: MeidasTouch — {article title}`
• If the item provides a more direct link to a court order, filing, or official release, use that instead.

OUTPUT HANDOFF:
• After applying this MeidasTouch-specific guidance, follow the ATTACKS FIELD instruction and the Canonical Extraction Protocol that follow this preface.
• Do not restate output format rules here — the canonical block defines the single authoritative schema for labels, order, and the required `attacks: [...]` line.
""".strip()

# =======================================================================

PREFACE_NOAH: str = """
SOURCE: Noahpinion (Substack).
TYPE: Economic and political analysis essays written by economist Noah Smith.
STYLE: Narrative articles blending data-driven explanation with references to concrete government, institutional, and policy actions.
AUDIENCE: Readers seeking evidence-based, pro-democracy, economically literate insight into public governance and institutional performance.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• The full article body is fetched for event extraction; ignore formatting, images, and embeds.
• Each article typically intermixes explanation with factual references to policy, regulation, lawmaking, or official acts.

TEXT CHARACTERISTICS:
• A typical piece references 5–15 major developments in policy, governance, or economics.
• Some paragraphs describe concrete government or institutional acts (legislation, regulation, rulings, appointments); others are analytical.
• Extract only concrete, factual acts — exclude theory, commentary, or speculative prediction.
• Each qualifying act must yield one distinct event.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: capture every democracy-relevant act mentioned — one event per act.  
   A normal article yields 5–15 events depending on density.
2. Selection: include verifiable actions, rulings, directives, votes, appointments, sanctions, investigations, or official reports that affect governance, law, or rights.  
   Exclude opinion, forecasts, or counterfactual discussion.
3. Granularity: do not merge multiple acts within one paragraph. Each new governmental decision, policy, or ruling = one event.
4. Completeness: ensure no qualifying act is omitted.
5. Neutrality: past tense, factual, reportorial tone — no adjectives or interpretive framing.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the twelve canonical Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
If an act spans multiple areas (e.g., monetary policy and legislation), pick the most immediate substantive domain.

DATE RULE:
• Default to the article’s `post_date`.
• If a clear, newer specific date appears (“On Tuesday, the Fed raised rates…”), use that.
• Ignore dates tied only to historical or contextual examples.

SOURCE LINE:
• Attribute as: `Source: Noahpinion — {article title}`.
• If the article links directly to an official release, report, or filing, use that URL instead.

OUTPUT HANDOFF:
• After applying this Noahpinion-specific guidance, follow the ATTACKS FIELD instruction and the Canonical Extraction Protocol that follow this preface.
• Do not restate output format or footer rules here — those are fully defined in the canonical protocol block.
""".strip()

# =======================================================================

PREFACE_50501: str = """
SOURCE: 50501 (Substack).
TYPE: Curated protest / civil-society / campus / movement / repression roundups.
STYLE: Dense, multi-item posts listing real-world actions, police responses, legal constraints, and government measures that change the conditions for dissent.
AUDIENCE: Readers tracking on-the-ground democratic pressure and the state’s response to it.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• Full article body is fetched for extraction; formatting may be mixed (bullets, short lines, paragraphs).
• Treat every discrete action as potentially extractable, even if local or municipal.

TEXT CHARACTERISTICS:
• A typical 50501 post contains 8–25 concrete developments: protests held/planned, permits granted or denied, arrests or detentions, police/campus directives, city council restrictions, counter-protest actions, speech or space rules, platform takedowns related to organizing.
• Some items will be pure commentary or “here’s what people are saying” — ignore those unless they report a new official act.
• Many items concern local jurisdictions (city, county, campus, school district). Keep them if the text shows a real act that affects protest, assembly, or expression.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract every democracy-relevant act — one event per act. If a paragraph contains both “city imposed a protest ban” and “police cleared the square,” that is two events.
2. Selection (INCLUDE): actual protests held; permits or approvals; bans/curfews/space restrictions; arrests, detentions, or charges linked to protest; executive/legislative moves to criminalize or protect protest; university/agency speech rules; campus discipline tied to protest; police or DHS tasking against demonstrators; cyber/platform actions that target organizing.
3. Selection (EXCLUDE): analysis, background, fundraising, “people are angry,” general atmosphere, or predictions with no new act.
4. Granularity: do not merge multiple acts reported together — each discrete step is its own event.
5. Neutrality: past-tense, factual, reportorial tone; no approval/condemnation.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the twelve Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
• Most 50501 items will fall in: 6 (Civil Society & Protest), 4 (Law Enforcement & Surveillance), 7 (Information & Media Control), or 10 (Transparency & Records).
• If both protest and police enforcement appear, choose the domain most directly affected.

DATE RULE:
• Default to the post’s `post_date`.
• If the item clearly names a more specific/later date (“On 10/29 police…”, “On Thursday the council…”), use that date.
• If the item announces an upcoming protest with a specific date, use that future date; otherwise, keep `post_date`.

SOURCE IDENTIFICATION:
• Attribute as: `Source: 50501 — {article title}`.
• If the item links to a better primary source (city order, campus memo, police press release), use that URL instead.

OUTPUT HANDOFF:
• After applying this 50501-specific guidance, follow the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• 50501 items often *are* attacks (bans, arrests, credential pulls, campus sanctions) — do not skip the `"attacks": [...]` line; use the handles that best match the repression (e.g. `dissent_protest`, `law_enforcement`, `transparency`, `information_control`, `civil_service` if it is an employment/discipline move).
""".strip()

# =======================================================================

PREFACE_OUTLOUD: str = """
SOURCE: Outloud (Substack, by Katrina Ziegler).
TYPE: Commentary and analysis focused on media, information control, transparency, and the democratic function of communication.
STYLE: Narrative essays blending political analysis with concrete references to government actions, censorship, whistleblowing, and institutional media behavior.
AUDIENCE: Readers examining how information ecosystems, press freedom, and speech policy shape democracy and public accountability.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• Full article body is fetched for extraction; ignore quotations, images, or embeds.
• Treat all visible text as potential content — formatting may vary between essays.

TEXT CHARACTERISTICS:
• Each article typically references 5–15 discrete acts: new laws, censorship measures, transparency rulings, whistleblower reprisals, press restrictions, or institutional reforms.
• Analytical framing is common — extract only concrete, verifiable actions or decisions that alter rights, access, or information flow.
• Each qualifying act becomes one event.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract every democracy-relevant act — one event per act.  
   Typical yield: 5–15 event blocks per essay.
2. Selection (INCLUDE): legislation, executive orders, investigations, sanctions, arrests, or judicial decisions affecting information freedom, press rights, or protest communication.  
   Include policy directives on access, classification, or censorship.
3. Selection (EXCLUDE): opinion, speculation, moral framing, or analogies without a new factual act.
4. Granularity: do not merge acts; treat each official decision, enforcement, or communication policy as a distinct event.
5. Neutrality: past-tense, factual tone — no adjectives or advocacy language.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the twelve canonical Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
• Most Outloud items will fall under: 7 (Information & Media Control) or 10 (Transparency & Records); occasionally 6 (Civil Society & Protest) or 4 (Law Enforcement & Surveillance).

DATE RULE:
• Default to the article’s `post_date`.
• If a newer, specific date is cited for an action (“On Tuesday the DOJ…”), use that date.
• Ignore historical or contextual references unless tied to a newly reported act.

SOURCE LINE:
• Attribute as: `Source: Outloud — {article title}`.
• If a primary-source link (court filing, FOIA release, or official directive) is cited, use that URL instead.

OUTPUT HANDOFF:
• After applying this Outloud-specific guidance, follow the ATTACKS FIELD instruction and the Canonical Extraction Protocol that follow this preface.
• Do not restate format or footer rules here — those are fully defined in the canonical block.
""".strip()

# =======================================================================

PREFACE_HCR: str = """
SOURCE: Letters from an American (Heather Cox Richardson, Substack).
TYPE: Daily historical essays connecting current political, legal, and institutional developments to democratic principles and historical context.
STYLE: Narrative prose that weaves factual reporting with interpretive framing; factual acts are embedded within analysis rather than isolated as bullet points.
AUDIENCE: Civically engaged readers seeking historical grounding for current events and the functioning of democracy.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• Full article text is fetched for extraction; if `type="podcast"`, use the transcript field instead.
• Ignore multimedia, links, or footnotes; extract only from visible narrative text.

TEXT CHARACTERISTICS:
• Each essay typically references 8–20 factual acts—votes, rulings, filings, executive actions, or public statements—interwoven with historical commentary.
• Analytical or historical material is common; extract only verifiable, concrete actions.
• Each discrete act becomes one event.
• Factual density and narrative continuity require careful separation of commentary from act.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract *every* democracy-relevant act — one event per act.
   A typical HCR essay yields 8–20 event blocks.
2. Selection: include all verifiable governmental or institutional actions affecting law, governance, rights, accountability, or democratic norms.
   Exclude interpretation, historical parallels, or rhetorical analysis unless necessary to identify the factual act.
3. Granularity: do not merge distinct acts in a single paragraph. Each signing, ruling, order, vote, or official announcement = one event.
4. Completeness: ensure no qualifying act is omitted, even if it appears within long sentences or dependent clauses.
5. Neutrality: factual, past-tense, reportorial tone only — no evaluative or emotive language.
6. Historical sensitivity: distinguish between *past-context reference* and *newly described current act*; only the latter qualifies as an event.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the twelve canonical Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
• Most HCR items fall under: 1 (Executive Actions & Orders), 2 (Legislative & Oversight Activity), 3 (Judicial Developments), or 10 (Transparency & Records).

DATE RULE:
• Default to the article’s `post_date`.
• If the essay clearly cites a newer, specific action date (“On Tuesday the Court…”), use that date.
• Ignore historical references or long-past context used only for illustration.

SOURCE LINE:
• Attribute as: `Source: Letters from an American — {article title}`.
• If the essay cites an authoritative primary source (court filing, transcript, official order), use that URL instead.

OUTPUT HANDOFF:
• After applying this HCR-specific guidance, follow the ATTACKS FIELD INSTRUCTION and the Canonical Extraction Protocol that follow this preface.
• Do not restate format or footer rules here — those are fully defined in the canonical block.
""".strip()

# =======================================================================

PREFACE_POPINFO: str = """
SOURCE: Popular Information (Substack).
TYPE: Investigative newsletter focused on political, corporate, and ethical accountability.
STYLE: Deep-dive reporting that uncovers specific acts (regulatory actions, secret agreements, campaign-finance maneuvers, internal directives, retaliation, or misuse of public power) and documents them with primary sources.
AUDIENCE: Readers who want evidence-backed reporting about how power is actually being exercised.

DATA INPUTS:
• JSON provides `title`, `url`, and `post_date`.
• Full article body is fetched for extraction; ignore embedded graphics, social embeds, and newsletter boilerplate.
• Treat documents, emails, filings, and internal memos cited in the piece as primary evidence that may define the event.

TEXT CHARACTERISTICS:
• Most issues contain 1–5 major investigative findings, but each finding can contain several discrete acts once unpacked.
• Reporting often blends fact + analysis — you must strip the analysis and extract only the verifiable act(s).
• Underlying acts may have occurred earlier but are newly revealed here; treat those as fresh, democracy-relevant events for the week.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract every concrete, verifiable act or revelation — one event per act. A typical piece will yield 2–8 events.
2. Selection: include actions that change law, policy, access, enforcement, oversight, transparency, election administration, or that document abuse/corruption by public officials or entities doing public work.
3. Granularity: keep distinct acts distinct. “Company funded the event” and “Agency bent rules to allow it” are two events.
4. Evidence-aware summaries: because these are investigative stories, each Summary may need to be slightly longer (≈70–110 words) to name the actor, the act, and the evidence (“according to internal emails obtained by Popular Information…”).
5. Neutrality: formal, evidence-based tone; do not imply motive or criminality beyond what the article states.
6. Recency/revelation rule: if the piece is *revealing* a past act for the first time, date it with `post_date` but note the original dated act if the article makes it explicit.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the twelve Democracy Clock policy domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
• Popular Information pieces often end up in 8 (Economic & Regulatory Power), 10 (Transparency & Records), 2 (Legislative & Oversight Activity), or 4 (Law Enforcement & Surveillance), but pick the most directly affected domain.

DATE RULE:
• Default to the article’s `post_date`.
• If the article clearly names the actual action date (“On May 5, the FEC…”) use that date for that event.
• If the action happened earlier but is only now revealed, keep the event dated at `post_date` and parenthetically note the original date if explicit: (occurred 2025-03-03).

SOURCE LINE:
• Prefer the most specific, authoritative URL cited in the article (filing, docket, leaked doc, FEC record, agency PDF).
• If none is given, use: `Source: Popular Information — {article title}`.

OUTPUT HANDOFF:
• After applying this Popular Information–specific guidance, apply the ATTACKS FIELD INSTRUCTION (global Trump-Attacks handles) and then the Canonical Extraction Protocol.
• Do not restate schema, labels, or footer here; the canonical block defines the exact output format.
""".strip()

# =======================================================================

ATTACKS_PREFACE = """
ATTACKS FIELD (MANDATORY — 55 STANDARD CATEGORIES)
For every event, you must decide whether the act described is an “attack” on people, rights, institutions, 
truth, the international order, or the Constitution, using the following fixed list of 55 categories. 
Output the line:

  attacks: [handle_1, handle_2, …]

for EVERY event. If none apply, output

  attacks: []

Do not skip the line.

WHEN TO TAG AN ATTACK
• Tag an attack when the act narrows rights, access, transparency, protest, immigration/asylum, voting, 
  independent oversight, or fair/equal administration of law.
• Tag an attack when the act uses government or political power to punish, coerce, retaliate, or extract 
  personal/partisan benefit.
• Tag an attack when the act degrades the capacity of democratic institutions to function (courts, Congress, 
  civil service, IGs, intel, diplomacy).
• Tag an attack when the act undermines shared fact, press freedom, or the information environment.
• Tag an attack when the act strikes at constitutional structure (separation of powers, peaceful transfer, 
  amendments).
• If the act is remedial, protective, or clearly the reverse (expanding access, restoring transparency, 
  protecting dissent), use: attacks: []

HOW MANY TO TAG
• 0 is allowed → attacks: []
• 1–3 is normal
• 4+ only when the text clearly shows multiple victim groups or multiple institutional targets

THE 55 CANONICAL ATTACK HANDLES
(Use these exact spellings; do not invent new ones.)

PART I – The People He Harmed
1. children — attacks on children, students, dependent minors (e.g. SNAP/care cuts that hit kids first)
2. women — attacks on women’s rights, status, or autonomy
3. minorities — attacks on racial, ethnic, or religious minorities
4. immigrants_refugees — attacks on immigrants, migrants, refugees, asylum seekers, family separation, spectacle at the border
5. lgbtq — attacks on LGBTQ+ people and protections
6. workers — attacks on labor, wages, unions, bargaining power
7. poor — attacks on the poor, food/housing insecurity, weaponized benefits
8. veterans — attacks on or neglect of veterans and service access
9. disabled — attacks on people with disabilities or disability supports
10. sick_vulnerable — attacks on the medically vulnerable, serious public-health neglect

PART II – The Nation He Degraded
11. truth — attacks on fact, honesty, reality-based governance
12. science — suppression or distortion of science and evidence
13. education — censorship/defunding/indoctrination in education
14. culture_art — turning culture/art into propaganda or loyalty tests
15. public_memory — rewriting history, erasing conscience, imposed mythology
16. faith — weaponizing religion, redefining morality as obedience
17. decency — normalization of cruelty, corruption, contempt
18. hope — deliberate cultivation of despair or futility

PART III – The Institutions He Broke
19. presidency — monetizing the presidency, destroying norms of restraint
20. courts — defying, packing, or hollowing the courts
21. congress — contempt for, or obstruction of, Congress and oversight
22. civil_service — purging, politicizing, or loyalty-testing the civil service
23. justice_dept — turning DOJ/law enforcement into a personal shield or weapon
24. intelligence — discrediting intel community, empowering conspiracists
25. military — politicizing or misusing the military, oaths to men not laws
26. diplomacy — gutting diplomacy, sidelining State, damaging alliances
27. ig_watchdogs — removing or disabling inspectors general and watchdogs
28. public_service — converting public service into a profit/extraction center

PART IV – The Truth He Erased
29. press — censorship, intimidation, or capture of the press
30. information — gag orders, secrecy, propaganda ecosystems
31. whistleblowers — retaliation/exposure/criminalization of conscience
32. internet — manipulation, deplatforming for loyalty, disinformation infra
33. knowledge — deleting data, reports, archives, inconvenient findings
34. reality — building a parallel universe in which nothing true survives

PART V – The World He Unmade
35. allies — betrayal or coercion of allies (NATO, G7, partners)
36. global_democracy — siding with/autocratizing abroad, abandoning rights
37. trade — weaponizing trade/tariffs/commerce for political reward/punishment
38. peace — withdrawals/war-brinkmanship that destabilize peace
39. climate_cooperation — abandoning climate agreements and joint action
40. idea_of_america — turning the U.S. from beacon to bludgeon

PART VI – The Republic Itself
41. constitution — treating the Constitution as optional/situational
42. separation_of_powers — executive supremacy over checks and balances
43. rule_of_law — loyalty as legality; impunity for in-group, punishment for out-group
44. emoluments — self-enrichment, grift, pay-to-play from public office
45. birthright_citizenship — undermining the 14th Amendment and belonging
46. amendment_22 — testing or eroding two-term limits
47. amendment_25 — shielding incapacity from constitutional remedy
48. peaceful_transfer — attacks on certification, succession, Jan 6-style tactics
49. union — threats/coercion toward states, sabotage of federalism

PART VII – The Future We Must Rebuild
50. environment — degradation of environment, land, protections
51. economy — crony capitalism, extraction of public good
52. public_health — pandemic denial, dismantled health infrastructure
53. civic_education — killing democratic literacy and informed citizenship
54. future — sacrificing long-term national interest for short-term power
55. reality_itself — ultimate authoritarian move: making the lie outlive the truth

FAILURE CONDITIONS
❌ Missing or omitted attacks: [...] on ANY event
❌ Using handles not in the 55-item list above
❌ Using prose (“this was an attack…”) instead of a bracketed list
❌ Narrative/blended output instead of canonical block
❌ Omission of the final footer (`Total events found: …` + `[END OF LOG]`)

NOTES
• This attacks section + the source preface + the Canonical Extraction Protocol are ONE inseparable instruction set.
• Builders must preserve the attacks field exactly as emitted.
• Downstream steps may fan-out or filter by attacks, so omissions here are data loss.

""".strip()

# =======================================================================

PREFACE_ZETEO: str = """
SOURCE: Zeteo — “This Week in Democracy” (Substack).
TYPE: Weekly civic-intelligence digest compiling democracy-related developments across government, law, media, elections, and civil society.
STYLE: Highly structured, high-density lists of short items, usually already grouped by domain (Power, Rights, Information, Elections, etc.).
AUDIENCE: Readers who need a complete weekly rollup of democratic movement — not a narrative, not a selection.

DATA INPUTS:
• JSON input provides `title`, `url`, and `post_date`.
• Full article body is fetched for extraction; ignore layout, embedded media, and section headers — treat all visible text as content.
• Many items will already read like events; your job is to convert them into the Democracy Clock canonical format.

TEXT CHARACTERISTICS:
• A normal issue contains 20–40 discrete developments from the prior week.
• Items are short (1–3 sentences) and already factual.
• Items span the full democracy surface: executive actions, congressional moves, court orders, election administration, censorship/press, civil society, economic/regulatory power.
• Each item should normally become exactly one event.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract every democracy-relevant act or development in the issue — one event per described act. If the issue has 30 items, expect ≈30 events.
2. Selection: include only concrete, verifiable acts — new rulings, filings, orders, directives, votes, appointments/removals, censorship, arrests, protest restrictions, releases of key information, sanctions, deployments. Exclude commentary, summaries of prior weeks, or general analysis unless tied to a new act.
3. Granularity: do not merge separate acts even if they are in the same paragraph; “court blocked X” and “governor signed Y” are two events.
4. Completeness: short outputs (<20) are non-compliant unless the issue itself was abnormally short.

CATEGORY DISCIPLINE:
Assign each event to exactly one of the 12 canonical Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
If more than one domain could apply, pick the most immediate/substantive one (e.g. an order that changes ballot access → “Elections & Representation,” not “Judicial Developments”).

DATE RULE:
• Default to the issue’s `post_date`.
• If an item clearly says the act occurred on a later/explicit day (“On Thursday…”, “Earlier today…”, “On 10/29…”), use that date instead.
• Ignore dates that are purely historical/contextual.

SOURCE LINE:
• Attribute as: `Source: Zeteo — This Week in Democracy — {article title}`
• If an item includes a superior primary link (court PDF, agency release, state order), use that link instead of the article URL.

OUTPUT HANDOFF:
• After applying this Zeteo-specific guidance, follow the ATTACKS FIELD instruction and the Canonical Extraction Protocol that follow this preface.
• Do not restate output rules here; the canonical block is the single source of truth for labels, order, footer, and the required `attacks: [...]` line.
""".strip()


# =======================================================================


PREFACE_CONGRESS: str = """
SOURCE: Congress.gov (U.S. Legislative Actions)
TYPE: Structured legislative records describing terminal or near-terminal outcomes — passage, veto, enactment, failure, or formal receipt between chambers.
STYLE: Formal institutional entries summarizing the status of bills and resolutions using standardized fields from Congress.gov.
AUDIENCE: Researchers documenting the official legislative life-cycle of federal measures and their democratic consequences.

DATA INPUTS:
• Each record includes `title`, `url`, `post_date`, and enriched fields (LATEST_ACTION, ACTIONS[], SUMMARY).
• When enriched fields are present, they override any shorter status text.
• Each record represents ONE discrete act of Congress — emit exactly ONE event.

TEXT CHARACTERISTICS:
• Structured data, not prose. Fields reflect the most recent or dispositive action (e.g., “Became law,” “Vetoed,” “House passed,” “Received in Senate”).
• May contain multiple dated actions; the newest authoritative one defines the event.
• No commentary or interpretation — your task is factual extraction only.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: output exactly one event per record.
2. Selection: describe the operative action reflected in the latest authoritative Congress.gov field — not intermediate steps.
3. Title format: `{date} — {Outcome}: {Bill Title}` (e.g., `2025-01-04 — Became Law: VETS Safe Travel Act`).
4. Summary: ≤35 words; specify who acted, what happened, and the democratic or policy effect.
5. Neutrality: no opinion, motive, or commentary; factual phrasing only.

CATEGORY DISCIPLINE:
Assign each bill’s action to the single substantive policy domain most directly affected — not to “Legislative & Oversight Activity” by default.  
Use the twelve Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
Examples:
• A voting-law change → “Elections & Representation.”  
• A defense authorization → “Civil–Military Relations & State Violence.”  
• A whistleblower-protection expansion → “Transparency & Records.”

DATE RULE:
• Prefer the explicit date in the LATEST_ACTION or most recent ACTION item.
• If none exists, default to the record’s `post_date`.

SOURCE LINE:
• Use the specific Congress.gov permalink for that bill; format:  
  `Source: Congress.gov — {bill title}`.

OUTPUT HANDOFF:
• After applying this Congress-specific guidance, apply the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• Do not restate schema, label, or footer rules here; those are fully defined in the canonical block.
""".strip()

# =======================================================================

PREFACE_FEDERAL_REGISTER: str = """
SOURCE: Federal Register (Official U.S. Government Gazette)
TYPE: Structured notices of completed executive and agency actions — final rules, orders, proclamations, notices, or enforcement decisions.
STYLE: Formal administrative language describing finalized legal or regulatory acts, not media coverage.
AUDIENCE: Analysts tracking the lawful exercise of executive and regulatory power in official form.

DATA INPUTS:
• Each record represents one published Federal Register entry (e.g., final rule, proclamation, notice, enforcement order).
• Fields include `title`, `url`, `post_date`, and structured metadata from the document header.
• Each record = ONE event. Do not create sub-events or commentary.

TEXT CHARACTERISTICS:
• Entries are already factual and procedural — often short, formulaic, and precise.
• Each item describes a single completed act of government: what authority was invoked, what rule or decision was made, and what it changes or enforces.
• The objective is not interpretation but accurate restatement in canonical event format.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: emit exactly one event per record.
2. Selection: focus on the *operative act* (final rule, decision, proclamation, order, etc.), not preambles or background.
3. Summary: concise (≤35 words), fact-based, naming WHO acted, WHAT changed, and the scope or effect if relevant.
4. Neutrality: formal administrative tone; do not speculate or interpret.
5. Completeness: ensure that each event fully communicates the nature, actor, and democratic significance of the action.

CATEGORY DISCIPLINE:
Assign the event to the substantive policy domain most directly affected by the act, not the process of publication.  
Choose from the twelve Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
Examples:
• A voting-registration rule → “Elections & Representation.”
• A trade tariff adjustment → “Economic & Regulatory Power.”
• A proclamation restricting speech access → “Information & Media Control.”

DATE RULE:
• Use the document’s publication date (`post_date`), unless a specific “effective date” or “signed date” is explicitly given and better represents the act itself.
• If both appear, prefer the effective date for rules, and the signed date for proclamations or orders.

SOURCE LINE:
• Use the official Federal Register URL or direct PDF link. Format as:  
  `Source: Federal Register — {document title}`

OUTPUT HANDOFF:
• After applying this Federal Register–specific guidance, apply the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• Do not restate schema, label, or footer rules here; those are fully defined in the canonical block.
""".strip()

# =======================================================================

PREFACE_DEMOCRACY_DOCKET: str = """
SOURCE: Democracy Docket (election-law and voting-rights litigation tracker).
TYPE: Case-based reporting on concrete developments in voting, redistricting, election administration, and related democracy litigation.
STYLE: Procedural, court-centered updates that often bundle several filings or orders from the same dispute.
AUDIENCE: Researchers documenting how litigation is changing access to the ballot, map fairness, and election rules in real time.

DATA INPUTS:
• Input provides `title`, `url`, `post_date`, and the full Democracy Docket post text.
• A single post may report MULTIPLE distinct acts (e.g., new complaint + preliminary injunction + notice of appeal).
• Each concrete act must become its own event. This source is MANY-ACTS-PER-POST.

TEXT CHARACTERISTICS:
• Most posts describe 2–8 specific actions in one case, or several related cases in the same state.
• Actions are typically: court orders (grant/deny stay, PI, TRO, merits ruling), party filings (complaint, intervention, appeal), compliance actions (adopting a new map, changing certification), or election-admin steps taken because of the litigation.
• Narrative/context is helpful but must not be extracted as events.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract EVERY concrete, democracy-relevant act described — one event per act. If the post reports 5 distinct filings/orders, output ≈ 5 events.
2. Selection: INCLUDE court orders, merits opinions, injunctions, stays, denials, remands; new or amended complaints; motions to intervene; notices of appeal; settlements/consent decrees; adoption of new maps; election-rule changes taken in response to the case.
3. Exclude: commentary on why the case matters, restatement of prior background, or media/political reaction with no new legal act.
4. Granularity: do not merge acts even if they occur in the same court on the same day — “panel grants stay” and “plaintiffs file notice of appeal” are two events.
5. Neutrality: report in factual, procedural tone (who acted, in what forum, and what changed for the election/voting/public body).

DATE RULE (OVERRIDE):
• If the post names the date of the act (“On Oct. 29 the panel…”, “Today the court…”, “On Friday the legislature…”), USE THAT DATE.
• Otherwise, default to the post’s `post_date`.
• If the post describes 3 acts on 3 dates, produce 3 events with 3 matching dates.

SOURCE LINE:
• Prefer the most specific litigation link available (court order, docket PDF, filing link).
• If none is present, use the Democracy Docket permalink for that post.
• Source must be ONE OR MORE plain URLs, space-separated, no labels, no markdown.

CATEGORY DISCIPLINE:
Choose EXACTLY ONE of the twelve Democracy Clock policy domains for every event:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
Note: most Democracy Docket events will fall under “Elections & Representation” or “Judicial Developments.” If the event directly changes how an election is run (maps, deadlines, ballot rules), prefer “Elections & Representation.”

OUTPUT HANDOFF:
• After applying this Democracy Docket–specific guidance, apply the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• Final output must therefore be a sequence of discrete event blocks — one per act — ending with:
  Total events found: [#]
  [END OF LOG]

FAILURE CONDITIONS:
❌ Fewer events than concrete acts described in the post
❌ Missing any required canonical fields (Summary, Source, Category, Why Relevant)
❌ Merged or narrative output instead of distinct event blocks
❌ Using markdown, bullets, or non-canonical labels
❌ Footer missing or malformed

EXPECTED RESULT:
A complete, multi-event log of every litigation or election-rule development described in the Democracy Docket post, normalized to Democracy Clock’s 12 domains and ready for Step-3 trait mapping.
""".strip()

# =======================================================================

PREFACE_ECON: str = """
SOURCE: Economics & Policy Roundups (multi-source).
TYPE: Aggregated summaries of official government actions, economic policy decisions, and regulatory announcements.
STYLE: Short-form institutional reporting mixing multiple verified actions with limited commentary or context.
AUDIENCE: Readers tracking how public power is exercised through economic governance and fiscal regulation.

DATA INPUTS:
• Input includes `title`, `url`, and `post_date`, with text drawn from multi-source economic or policy digests.
• Each article may contain numerous discrete acts — government announcements, agency rules, enforcement actions, or central-bank decisions.
• Treat every verifiable public action as a separate event. Commentary, forecasts, or market reactions are not events.

TEXT CHARACTERISTICS:
• Dense, factual summaries of official acts or releases.
• Typical sources: executive orders, agency rules, enforcement actions, sanctions/export controls, trade and tariff adjustments, antitrust rulings, procurement decisions, or emergency economic authorities.
• Also include legislative actions (passage, veto, disapproval) that alter economic policy; and judicial rulings that enable, block, or define regulatory scope.
• Include official statistical releases (BLS, BEA, Census, Fed) only if the article reports the **official publication**, not commentary about it.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract *every qualifying act* described — one event per act.
2. Selection: include government actions, legislation, court decisions, and central-bank announcements that materially affect law, regulation, or public power.
3. Exclude: commentary, forecasts, polls, private-market news, or analysis with no discrete act.
4. Granularity: do not merge acts; “President signed order” and “Fed raised rates” = two separate events.
5. Neutrality: concise, factual, past-tense tone with no adjectives or causal claims beyond what’s stated.
6. Completeness: a compliant extraction should yield multiple events (typically 5–20 per digest, depending on density).

DATE RULE:
• Prefer the official action or release date if explicitly stated in the text.
• Otherwise, use the article’s `post_date`.
• If the digest covers acts across several days, date each event individually when possible.

CATEGORY DISCIPLINE:
Assign each event to the most directly affected policy domain — not always “Economic & Regulatory Power.”  
Choose from the twelve Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
Examples:
• Tariffs or sanctions → “Economic & Regulatory Power” or “International Relations.”
• Labor rules → “Economic & Regulatory Power.”
• Funding rules for elections → “Elections & Representation.”
• Court rulings on regulation → “Judicial Developments.”

SOURCE IDENTIFICATION:
• Attribute as `Source: Economics & Policy Roundup — {article title}`.
• If the post cites a primary government document (rule, order, release, PDF), use that URL instead of the article link.

OUTPUT HANDOFF:
• After applying this Economics-specific guidance, apply the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• Do not restate schema, labels, or footer rules here — those are defined in the canonical section.

FAILURE CONDITIONS:
❌ Missing or merged qualifying acts
❌ Missing canonical fields (Summary, Source, Category, Why Relevant)
❌ Narrative or multi-paragraph output instead of discrete event blocks
❌ Missing or malformed footer (`Total events found` + `[END OF LOG]`)

EXPECTED RESULT:
A structured event log capturing all official government economic and regulatory actions described in the source, normalized to Democracy Clock standards and ready for Step-3 trait mapping.
""".strip()

# =======================================================================

PREFACE_ORDERS: str = """
SOURCE: Federal and state courts issuing emergency, interim, or merits orders.
TYPE: Single-action judicial directives (stays, injunctions, denials, remands) that immediately change what government or litigants may do.
STYLE: Docket-level procedural acts with minimal narrative but significant institutional impact.
AUDIENCE: Researchers documenting how judicial authority redefines or constrains executive, legislative, or electoral power.

DATA INPUTS:
• Each record provides `title`, `url`, and `post_date` (or decision date) and represents one operative judicial order.
• The source text may include the order’s effect, dissenting notes, or procedural posture.
• Each record = ONE event. Do not combine or split.

TEXT CHARACTERISTICS:
• Orders are terse but definitive — each describes a clear judicial action with immediate consequence.
• Typical actions: stay, injunction, denial, vacatur, remand, administrative stay, or other emergency relief.
• Contextual references (e.g., “pending appeal,” “Kagan and Sotomayor would deny”) may appear and should be included only as factual detail in the summary.
• Avoid speculation about motives or long-term implications.

EXPECTED OUTPUT BEHAVIOR:
1. Coverage: extract exactly one event per record — one court order, one output.
2. Selection: include only the operative judicial act (stay, vacate, grant, deny, remand).
3. Neutrality: factual, procedural, and precise; no interpretation or opinion.
4. Summary: identify who acted, what changed legally or procedurally, and the immediate practical effect.
5. Completeness: ensure the summary covers the nature of relief, affected policy area, and scope of effect.

CATEGORY DISCIPLINE:
Assign each event to the most directly affected substantive policy domain (not merely “Judicial Developments”).  
Choose from the twelve Democracy Clock domains:
1. Executive Actions & Orders
2. Legislative & Oversight Activity
3. Judicial Developments
4. Law Enforcement & Surveillance
5. Elections & Representation
6. Civil Society & Protest
7. Information & Media Control
8. Economic & Regulatory Power
9. Appointments & Patronage
10. Transparency & Records
11. International Relations
12. Civil–Military Relations & State Violence
Examples:
• An election-administration injunction → “Elections & Representation.”
• A stay reinstating an immigration ban → “Immigration Rights” (under “Executive Actions & Orders”).
• A ruling blocking protest restrictions → “Civil Society & Protest.”

DATE RULE:
• Use the order or decision date if provided; otherwise, use `post_date`.
• If the text specifies multiple relevant dates (e.g., filing vs. ruling), select the date of the operative order.

SOURCE IDENTIFICATION:
• Attribute as: `Source: Judicial Orders — {case name}` or use the most specific docket or PDF URL available.
• If the post links to multiple filings, prefer the one representing the operative order.

OUTPUT HANDOFF:
• After applying this Orders-specific guidance, apply the ATTACKS FIELD INSTRUCTION and then the Canonical Extraction Protocol that follow this preface.
• Do not restate canonical labels or footer rules here — those are defined in the canonical section.

FAILURE CONDITIONS:
❌ More than one event produced per record  
❌ Missing required canonical fields (Summary, Source, Category, Why Relevant)  
❌ Narrative prose instead of a discrete event block  
❌ Missing `[END OF LOG]` footer or incorrect total count

EXPECTED RESULT:
A single, structured event capturing the operative judicial act, who issued it, what changed procedurally or substantively, and why it matters democratically — ready for Step-2 archival and Step-3 trait mapping.
""".strip()

# =======================================================================

# Registry for easy lookup
_SUBSTACK_PREFACES: Dict[str, str] = {
    "meidas": PREFACE_MEIDAS,
    "hcr": PREFACE_HCR,
    "popinfo": PREFACE_POPINFO,
    "zeteo": PREFACE_ZETEO,
    "50501": PREFACE_50501,
    "noah": PREFACE_NOAH,
    "outloud": PREFACE_OUTLOUD,

    # Judicial / legal feeds
    "orders": PREFACE_ORDERS,

    # Shadow docket (SCOTUS emergency orders)
    "shadow": PREFACE_SHADOW,

    # Congress
    "congress": PREFACE_CONGRESS,

    # Federal Register
    "federalregister": PREFACE_FEDERAL_REGISTER,

    # Democracy Docket (litigation tracker)
    "democracydocket": PREFACE_DEMOCRACY_DOCKET,

    # The Economy
    "econ": PREFACE_ECON
}

# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------

# optional: central place for aliasing common variations
_PREFACE_ALIASES: dict[str, str] = {
    "federal-register": "federalregister",
    "federal_register": "federalregister",
    "democracy_docket": "democracydocket",
    "democracy-docket": "democracydocket",
    "congress.gov": "congress",
    "congress_gov": "congress",
    "fr": "federalregister",          # if any harvester uses the short form
    "dd": "democracydocket",
}

def get_prompt_preface(source_key: str | None) -> str:
    """
    Return the short source-specific preface if known, else the Substack default.
    Normalizes common aliases so builders can pass e.g. 'federal-register' or
    'democracy_docket' and still get the right preface.
    """
    if not source_key:
        return SUBSTACK_DEFAULT_PREFACE
    key = str(source_key).lower().strip()
    key = _PREFACE_ALIASES.get(key, key)
    return _SUBSTACK_PREFACES.get(key, SUBSTACK_DEFAULT_PREFACE)


def compose_system_prompt(source_key: str | None, *, include_attacks: bool = True) -> str:
    """
    Compose the full system prompt for builders in COPY mode.

    Order is deliberate:
      1. Source-specific preface
      2. Global ATTACKS field instructions
      3. Canonical Extraction Protocol

    Builders must use the prompt as returned, without adding or altering text.
    This function produces the one authoritative system prompt for event extraction.
    """

    parts: list[str] = [get_prompt_preface(source_key)]
    parts.append(ATTACKS_PREFACE)
    parts.append(CANONICAL_EXTRACTION_PROTOCOL)
    return "\n\n".join(parts)