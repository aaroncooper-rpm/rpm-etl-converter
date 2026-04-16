"""
validation_panel.py
───────────────────
Step 2 tenant validation panel for the RPM Living Yardi ETL Converter.

Renders inside the 👥 Tenants tab of the Step 2 validate screen.
Consumes the vdata dict built by _build_vdata() in streamlit_app.py — no raw
DataFrames are needed here.

USAGE:
    from validation_panel import render_validation_panel
    render_validation_panel(v)          # v = st.session_state.vdata
"""

import pandas as pd
import streamlit as st


# ──────────────────────────────────────────────────────────────────────────────
# BUILD FLAGGED TENANT TABLE
# ──────────────────────────────────────────────────────────────────────────────

def _build_flagged_df(vdata: dict) -> pd.DataFrame:
    """
    Returns a DataFrame of tenants that have at least one data-quality issue.
    Each row includes five ⚑ issue columns; clean cells are empty strings.

    Issue flags:
        ⚑ Email      – no email found from any source
        ⚑ Phone      – no phone found from any source
        ⚑ Sign Date  – lease signed date missing
        ⚑ Floor Plan – floor plan not mapped in Takeover Guide
        ⚑ Rent       – lease rent differs from All Units market rent
    """
    tenants_full = vdata.get("tenants_full", [])
    if not tenants_full:
        return pd.DataFrame()

    # Build set of units with a rent discrepancy for fast lookup
    disc_units = {r["Unit"] for r in vdata.get("rent_discrepancies", [])}

    # Build set of unmapped floor plans
    unmapped_fps = {
        r["OneSite Code"]
        for r in vdata.get("unit_types", [])
        if r.get("Status", "") == "❌ Missing"
    }

    rows = []
    for t in tenants_full:
        e_flag  = "" if t.get("has_email")      else "✗ Missing"
        ph_flag = "" if t.get("has_phone")       else "✗ Missing"
        sg_flag = "" if t.get("has_sign_date")   else "✗ Missing"
        fp_flag = "✗ Unmapped" if t.get("floorplan", "") in unmapped_fps else ""
        rn_flag = "✗ Discrepancy" if t.get("Unit", "") in disc_units else ""

        if any([e_flag, ph_flag, sg_flag, fp_flag, rn_flag]):
            rows.append({
                "Unit":         t["Unit"],
                "Name":         t["Name"],
                "Status":       t["Status"],
                "Floor Plan":   t.get("floorplan", ""),
                "Lease Rent":   t["Rent"],
                "⚑ Email":      e_flag,
                "⚑ Phone":      ph_flag,
                "⚑ Sign Date":  sg_flag,
                "⚑ Floor Plan": fp_flag,
                "⚑ Rent":       rn_flag,
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# STYLING HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _red_if_flagged(val):
    if val and str(val).startswith("✗"):
        return "color:#c62828;font-weight:600;background-color:#ffebee"
    return ""

def _diff_colour(val):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    if v < 0:
        return "color:#c62828;font-weight:600;background-color:#ffebee"   # red  = below unit rent
    if v > 0:
        return "color:#e65100;font-weight:600;background-color:#fff3e0"   # amber = above unit rent
    return ""

def _badge(col, count: int, label: str):
    colour = "#d32f2f" if count > 0 else "#388e3c"
    col.markdown(
        f"<p style='font-size:1.4rem;font-weight:700;color:{colour};margin:0'>{count}</p>"
        f"<p style='font-size:0.75rem;color:#5a6a88;margin:0'>{label}</p>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# MAIN RENDER FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def render_validation_panel(vdata: dict) -> None:
    """
    Renders the full tenant data-quality panel for Step 2.

    Sections:
      1. Summary badges — count of each issue type
      2. Flagged tenants table — only tenants with at least one issue
      3. Resident Rent vs Unit Rent comparison table
    """

    flagged_df  = _build_flagged_df(vdata)
    rent_disc   = vdata.get("rent_discrepancies", [])
    total       = len(vdata.get("tenants_full", []))
    issue_cols  = ["⚑ Email", "⚑ Phone", "⚑ Sign Date", "⚑ Floor Plan", "⚑ Rent"]

    cnt_email   = int((flagged_df["⚑ Email"]      == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_phone   = int((flagged_df["⚑ Phone"]      == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_sign    = int((flagged_df["⚑ Sign Date"]  == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_fp      = int((flagged_df["⚑ Floor Plan"] == "✗ Unmapped"     ).sum()) if not flagged_df.empty else 0
    cnt_rent    = len(rent_disc)
    cnt_flagged = len(flagged_df)

    # ── 1. Summary badges ─────────────────────────────────────────────────────
    st.caption(
        "Only tenants with at least one data-quality issue are shown. "
        "The full roster is in the **Validation Report** workbook downloaded with Phase 1."
    )

    b0, b1, b2, b3, b4, b5 = st.columns(6)
    colour0 = "#d32f2f" if cnt_flagged > 0 else "#388e3c"
    b0.markdown(
        f"<p style='font-size:1.4rem;font-weight:700;color:{colour0};margin:0'>{cnt_flagged}</p>"
        f"<p style='font-size:0.75rem;color:#5a6a88;margin:0'>of {total} tenants flagged</p>",
        unsafe_allow_html=True,
    )
    _badge(b1, cnt_email,  "Missing Email")
    _badge(b2, cnt_phone,  "Missing Phone")
    _badge(b3, cnt_sign,   "Missing Sign Date")
    _badge(b4, cnt_fp,     "Unmapped Floor Plan")
    _badge(b5, cnt_rent,   "Rent Discrepancy")

    st.divider()

    # ── 2. Flagged tenants table ───────────────────────────────────────────────
    st.markdown("#### 🚩 Flagged Tenants")

    if flagged_df.empty:
        st.success("✅ No data-quality issues found across all tenant records.")
    else:
        with st.expander("Filter by issue type", expanded=False):
            issue_value_map = {
                "⚑ Email":      "✗ Missing",
                "⚑ Phone":      "✗ Missing",
                "⚑ Sign Date":  "✗ Missing",
                "⚑ Floor Plan": "✗ Unmapped",
                "⚑ Rent":       "✗ Discrepancy",
            }
            selected = st.multiselect(
                "Show only tenants with these issues (leave blank for all flagged):",
                options=issue_cols,
                default=[],
            )

        filtered = flagged_df.copy()
        if selected:
            mask = pd.Series(False, index=filtered.index)
            for col in selected:
                mask |= (filtered[col] == issue_value_map[col])
            filtered = filtered[mask]

        st.caption(f"Showing **{len(filtered)}** flagged tenant(s).")
        st.dataframe(
            filtered.style
                .map(_red_if_flagged, subset=issue_cols)
                .format({"Lease Rent": "${:,.0f}"}, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ── 3. Rent comparison ────────────────────────────────────────────────────
    st.markdown("#### 💲 Resident Rent vs. Unit Rent")
    st.caption(
        "Compares each resident's **Lease Rent** (Rent Roll) against the "
        "**Unit Rent** for their specific unit (Final All Unit report)."
    )

    if not rent_disc:
        st.success("✅ All resident lease rents match the unit rents in the All Units report.")
    else:
        rent_df = pd.DataFrame(rent_disc)
        over    = int((rent_df["Difference"] > 0).sum())
        under   = int((rent_df["Difference"] < 0).sum())

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Discrepancies", len(rent_df))
        m2.metric("Above Unit Rent",     over,  delta=f"+{over}",  delta_color="off")
        m3.metric("Below Unit Rent",     under, delta=f"-{under}", delta_color="inverse")

        st.dataframe(
            rent_df.style
                .map(_diff_colour, subset=["Difference"])
                .format({
                    "Unit Rent":  "${:,.2f}",
                    "Lease Rent": "${:,.2f}",
                    "Difference": "${:+,.2f}",
                    "% Diff":     "{:+.1f}%",
                }),
            use_container_width=True,
            hide_index=True,
        )

        with st.expander("What does this mean?"):
            st.markdown(
                "- **Negative difference** (red) — tenant is paying *below* the unit's market rent. "
                "Verify whether a concession, employee credit, or model-unit rate is intentional "
                "before import (charge codes: `concmgr`, `emplcred`, `model`).\n"
                "- **Positive difference** (amber) — tenant is paying *above* the unit's market rent. "
                "Confirm the All Units report rent is correct for this unit.\n"
                "- These discrepancies will be visible in Yardi after import. Intentional variances "
                "should be handled with the correct charge code rather than adjusting the base rent."
            )
