"""LMER-style mixed effects models (Python MixedLM).

Requires statsmodels. If not available, print guidance and exit.

Models:
  CES score ~ PM2.5 + Ozone + Traffic + Year (fixed effects)
  Random intercept by county
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir


def main() -> None:
    try:
        import pandas as pd
        import numpy as np
        import statsmodels.formula.api as smf
    except Exception as e:
        print("statsmodels is required for mixed effects models.")
        print("Install with: pip install statsmodels")
        print(f"Import error: {e}")
        return

    out_dir = ensure_out_dir("ancillary", "tables")
    notes_dir = ensure_out_dir("ancillary", "notes")

    df = load_data()
    # Keep core predictors and drop NA
    cols = ["ces_score", "pm25", "ozone", "traffic", "county", "year"]
    work = df[cols].copy()
    for c in ["ces_score", "pm25", "ozone", "traffic"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    work = work.dropna(subset=["ces_score", "pm25", "ozone", "traffic", "county", "year"])

    # Standardize predictors for stability
    for c in ["pm25", "ozone", "traffic"]:
        work[c] = (work[c] - work[c].mean()) / work[c].std()

    # Mixed effects model with random intercept by county
    model = smf.mixedlm(
        "ces_score ~ pm25 + ozone + traffic + C(year)",
        work,
        groups=work["county"],
    )
    result = model.fit(method="lbfgs", maxiter=200)

    # Save summary
    summary_txt = notes_dir / "ancillary_lmer_summary.txt"
    with summary_txt.open("w") as f:
        f.write(result.summary().as_text())

    # Save coefficients table
    coef = result.params.rename("coef")
    se = result.bse.rename("se")
    tvals = result.tvalues.rename("t")
    pvals = result.pvalues.rename("p")
    table = (
        coef.to_frame()
        .join(se)
        .join(tvals)
        .join(pvals)
        .reset_index()
        .rename(columns={"index": "term"})
    )
    out_csv = out_dir / "ancillary_lmer_coefficients.csv"
    table.to_csv(out_csv, index=False)

    print(f"Wrote {summary_txt}")
    print(f"Wrote {out_csv}")


if __name__ == "__main__":
    main()
