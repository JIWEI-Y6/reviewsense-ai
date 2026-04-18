"""Compare eval_results_*.json files from a bake-off and produce an HTML report.

Reads every eval_results_<model>.json in the eval/ directory, builds:
  1. A summary comparison table (rows = models, cols = metrics, winner bold per col)
  2. A per-case tool trace table (rows = questions, cols = models; shows tools used + pass/fail dimensions)

Usage:
    python -m eval.compare_models
    python -m eval.compare_models --out eval/comparison_report.html

The first view is for a project-report headline; the second is for diagnosing
why a given candidate failed (wrong tool chosen, synthesis missed the fact, etc.).
"""

import argparse
import glob
import html
import json
import os
from datetime import datetime


METRIC_KEYS = [
    ("intent_accuracy", "Intent Acc", True, "pct"),
    ("data_correctness", "Data Correct", True, "pct"),
    ("avg_factuality", "Factuality", True, "num"),
    ("avg_completeness", "Completeness", True, "num"),
    ("avg_citation_quality", "Citation", True, "num"),
    ("avg_context_utilization", "Context", True, "num"),
    ("hallucination_rate", "Hallu Rate", False, "pct"),
    ("fallback_rate", "Fallback Rate", False, "pct"),
    ("latency_p95", "P95 Latency (s)", False, "num"),
    ("avg_cost_per_query", "Avg $/Q", False, "cost"),
]


def _fmt(val, kind):
    if val is None:
        return "-"
    if kind == "pct":
        return f"{val * 100:.1f}%"
    if kind == "cost":
        return f"${val:.4f}"
    return f"{val:.2f}"


def find_winners(runs: list[dict]) -> dict[str, str | None]:
    """Return {metric_key: winning_model_name}. Ties → first seen."""
    winners = {}
    for key, _label, higher_better, _kind in METRIC_KEYS:
        best_model = None
        best_val = None
        for r in runs:
            v = r["summary"].get(key)
            if v is None:
                continue
            if best_val is None or (higher_better and v > best_val) or (not higher_better and v < best_val):
                best_val = v
                best_model = r["model"] or "default"
        winners[key] = best_model
    return winners


def render_summary_table(runs: list[dict]) -> str:
    winners = find_winners(runs)
    header = (
        "<tr><th>Model</th><th>Judge</th><th>N</th>"
        + "".join(f"<th>{html.escape(label)}</th>" for _k, label, _h, _t in METRIC_KEYS)
        + "</tr>"
    )
    rows = []
    for r in runs:
        model = r["model"] or "default"
        cells = [f"<td><b>{html.escape(model)}</b></td>"]
        cells.append(f"<td>{html.escape(r.get('judge_model') or '-')}</td>")
        cells.append(f"<td>{r['summary'].get('evaluated', '-')}</td>")
        for key, _label, _higher, kind in METRIC_KEYS:
            v = r["summary"].get(key)
            formatted = _fmt(v, kind)
            is_winner = winners.get(key) == model and v is not None
            cells.append(f"<td class='{'win' if is_winner else ''}'>{formatted}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return "<table class='summary'>" + header + "".join(rows) + "</table>"


def render_per_case_trace(runs: list[dict]) -> str:
    """Build the per-question tool trace table (MealMind-inspired)."""
    # Collect question ids appearing in any run
    by_qid: dict[str, dict] = {}
    for r in runs:
        model = r["model"] or "default"
        for result in r.get("results", []):
            qid = result["id"]
            if qid not in by_qid:
                by_qid[qid] = {
                    "question": result["question"],
                    "expected_intent": result["expected_intent"],
                    "models": {},
                }
            by_qid[qid]["models"][model] = result

    if not by_qid:
        return "<p><i>No per-case data available.</i></p>"

    model_names = [r["model"] or "default" for r in runs]
    header_cells = ["<th>ID</th><th>Question</th><th>Expected</th>"]
    for m in model_names:
        header_cells.append(f"<th>{html.escape(m)}</th>")
    header = "<tr>" + "".join(header_cells) + "</tr>"

    rows = []
    for qid in sorted(by_qid.keys()):
        entry = by_qid[qid]
        cells = [
            f"<td>{html.escape(qid)}</td>",
            f"<td class='q'>{html.escape((entry['question'] or '')[:120])}</td>",
            f"<td>{html.escape(entry['expected_intent'] or '')}</td>",
        ]
        for m in model_names:
            res = entry["models"].get(m)
            if not res:
                cells.append("<td>-</td>")
                continue
            if res.get("api_error"):
                cells.append(f"<td class='err'>API_ERR</td>")
                continue
            tools = res.get("tools_used") or []
            intent_ok = "✓" if res.get("intent_correct") else "✗"
            data_ok = "✓" if res.get("data_correct") else "✗"
            fact = res.get("judge_factuality", "?")
            cite = res.get("judge_citation_quality", "?")
            tools_str = ", ".join(tools[:4]) or "-"
            css = "ok" if res.get("intent_correct") and res.get("data_correct") else "warn"
            cells.append(
                f"<td class='{css}'>"
                f"<div class='tools'>{html.escape(tools_str)}</div>"
                f"<div class='scores'>I{intent_ok} D{data_ok} F{fact} C{cite}</div>"
                f"</td>"
            )
        rows.append("<tr>" + "".join(cells) + "</tr>")

    return "<table class='trace'>" + header + "".join(rows) + "</table>"


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ReviewSense Bake-Off: {run_id}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, sans-serif; max-width: 1400px; margin: 20px auto; padding: 0 20px; color: #222; }}
  h1 {{ margin-bottom: 4px; }}
  .meta {{ color: #777; font-size: 13px; margin-bottom: 20px; }}
  table {{ border-collapse: collapse; margin: 14px 0; font-size: 13px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
  th {{ background: #f5f5f5; font-weight: 600; }}
  .summary td.win {{ background: #d4edda; font-weight: 700; }}
  .trace td.ok {{ background: #e8f5e8; }}
  .trace td.warn {{ background: #fff4e6; }}
  .trace td.err {{ background: #f8d7da; }}
  .trace td.q {{ max-width: 280px; overflow: hidden; text-overflow: ellipsis; }}
  .trace .tools {{ font-size: 11px; color: #333; }}
  .trace .scores {{ font-family: monospace; font-size: 11px; color: #555; margin-top: 3px; }}
  .legend {{ color: #777; font-size: 12px; margin-top: 8px; }}
</style>
</head>
<body>
  <h1>ReviewSense AI — Cortex Bake-Off</h1>
  <div class="meta">
    Run ID: <code>{run_id}</code> ·
    Judge: <b>{judge}</b> ·
    Generated: {generated} ·
    Models: {models}
  </div>

  <h2>Summary (winner bold per metric)</h2>
  {summary}
  <div class="legend">Green cell = best score in that column. Hallucination / fallback / latency / cost are inverted (lower is better).</div>

  <h2>Per-Case Tool Trace</h2>
  <div class="legend">Each cell shows: tool calls (top) and per-metric pass/score (bottom, format: I=intent D=data F=factuality C=citation).
  Green = intent+data both correct. Orange = partial. Red = API error.</div>
  {trace}
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        default=os.path.dirname(__file__),
        help="Directory containing eval_results_*.json (default: eval/)",
    )
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "comparison_report.html"),
        help="Output HTML path",
    )
    args = parser.parse_args()

    pattern = os.path.join(args.input_dir, "eval_results_*.json")
    files = sorted(glob.glob(pattern))
    # Filter out files from older runs — keep only the latest run_id present
    if not files:
        print(f"No eval_results_*.json files found in {args.input_dir}")
        print("Run: python -m eval.run_eval --models <model1> <model2> ...")
        return 1

    runs = []
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            runs.append(json.load(f))

    # Keep only the most recent run_id (in case multiple bake-offs have been run)
    if any("run_id" in r for r in runs):
        latest_run_id = max(
            (r["run_id"] for r in runs if r.get("run_id")),
            key=lambda rid: max(r["timestamp"] for r in runs if r.get("run_id") == rid),
            default=None,
        )
        runs = [r for r in runs if r.get("run_id") == latest_run_id]

    run_id = runs[0].get("run_id", "unknown")
    judge = runs[0].get("judge_model", "unknown")
    models = ", ".join(html.escape(r.get("model") or "default") for r in runs)

    html_out = HTML_TEMPLATE.format(
        run_id=html.escape(run_id),
        judge=html.escape(judge),
        generated=datetime.now().isoformat(timespec="seconds"),
        models=models,
        summary=render_summary_table(runs),
        trace=render_per_case_trace(runs),
    )

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_out)

    print(f"Wrote {args.out}")
    print(f"  Run ID: {run_id}")
    print(f"  Judge:  {judge}")
    print(f"  Models: {[r.get('model') for r in runs]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
