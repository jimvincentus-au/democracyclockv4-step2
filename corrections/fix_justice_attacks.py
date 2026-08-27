#!/usr/bin/env python3
"""
Surgical correction of attack handles on the justice events. No LLM.

TWO DEFECTS, both measured 2026-08-27 on the 548-event justice corpus:

1. VICTIM-OF-CRIME HANDLES. The extractor tagged routine prosecutions with the
   handles of the CRIME's victims, when the state act protected them. A CBP
   officer sentenced for assaulting a minor was tagged `children`; the
   murder-for-hire plot against journalist Masih Alinejad was tagged `press`.
   Attack handles name whom the GOVERNMENT ACT harms. A prosecution that
   protects the victim attacks no one -> attacks: [].

   Only the unambiguous cases are cleared. Records where state power IS directed
   at someone (Comey, SPLC, denaturalizations, policy shifts) are left alone.

2. NON-CANONICAL HANDLES. Raw list indices ("31", "42", "43", "45") and invented
   names (elections_representation, civil_society_protest, elderly_vulnerable,
   indigenous_rights) that are not among the 55 canonical handles. Indices map
   back deterministically; invented names are dropped.

Writes the two justice event files in place and prints a full change log.
--apply to write; default is a dry run.
"""
import argparse, json, re, sys
from pathlib import Path

S2 = Path("/Volumes/PRINTIFY24/Democracy Clock Automation/Step 2")
sys.path.insert(0, str(S2))
from step2_prompts_v4 import ATTACKS_PREFACE  # noqa: E402

CANON = set(re.findall(r"^\s*\d+\.\s+([a-z0-9_]+)\s+—", ATTACKS_PREFACE, re.M))
BY_NUM = {m.group(1): m.group(2) for m in
          re.finditer(r"^\s*(\d+)\.\s+([a-z0-9_]+)\s+—", ATTACKS_PREFACE, re.M)}

FILES = ["justice_events_2025-01-20_2026-08-14.json",
         "justice_events_2026-08-15_2026-08-21.json"]

# Titles whose handles name the crime's victims, not a target of state power.
# Matched on a distinctive fragment so the list is auditable.
CLEAR = [
    "Sanger police officer convicted",
    "hate crime and interstate threats against Sikh",
    "Savanna police officer sentenced",
    "CBP officer sentenced for abduction",
    "mass mailing fraud targeting elderly",
    "Guatemalan national for smuggling unaccompanied minor",
    "Ohio nursing home nonprofit",
    "Canadian national extradited and charged with mail fraud",
    "Kokomo police officer for sexual assault",
    "Kimberly-Clark agrees to pay",
    "Arizona extremist network leader",
    "Alabama men convicted of sex trafficking",
    "Online seller of infant formula",
    "murder-for-hire plot targeting journalist",
]

# Left deliberately untouched -- state power directed at a person or institution,
# or a policy shift. Recorded here so the decision is visible, not implicit.
KEEP_NOTE = [
    "Comey", "Southern Poverty Law Center", "denaturalization",
    "Civil Rights Fraud Initiative", "ATF announce regulatory reforms",
    "accelerated review process", "classified information transmission",
    "attempted assassination",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the files (default: dry run)")
    a = ap.parse_args()

    cleared = remapped = dropped = 0
    for fn in FILES:
        p = S2 / "artifacts" / "eventjson" / fn
        d = json.loads(p.read_text(encoding="utf-8"))
        for e in d["events"]:
            title = e.get("title") or ""
            atk = list(e.get("attacks") or [])
            if not atk:
                continue
            orig = list(atk)

            # 1. victim-of-crime handles
            if any(frag.lower() in title.lower() for frag in CLEAR):
                e["attacks"] = []
                cleared += 1
                print(f"  CLEAR  {title[:66]}\n         {orig} -> []")
                continue

            # 2. numeric indices -> canonical names; invented names dropped
            new = []
            for h in atk:
                if h in CANON:
                    new.append(h)
                elif h in BY_NUM:
                    new.append(BY_NUM[h]); remapped += 1
                    print(f"  REMAP  {title[:60]}\n         {h!r} -> {BY_NUM[h]!r}")
                else:
                    dropped += 1
                    print(f"  DROP   {title[:60]}\n         {h!r} (not canonical)")
            if new != orig:
                e["attacks"] = new

        if a.apply:
            p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\ncleared (victim-of-crime) : {cleared}")
    print(f"remapped (numeric index)  : {remapped}")
    print(f"dropped  (invented)       : {dropped}")
    print(f"\nleft untouched by design  : {', '.join(KEEP_NOTE)}")
    print("\nWRITTEN." if a.apply else "\nDRY RUN — nothing written. Re-run with --apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
