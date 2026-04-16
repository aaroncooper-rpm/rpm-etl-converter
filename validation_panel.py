"""
validation_panel.py
───────────────────
Drop-in replacement for the Step 2 tenant preview section.

USAGE — in your main Streamlit app, replace the existing 60-row tenant preview with:

    from validation_panel import render_validation_panel
    render_validation_panel(rent_roll_df, lease_details_df, contract_df,
                            unit_type_map, all_units_df)

Where:
    rent_roll_df      – parsed Final Rent Roll Detail DataFrame
    lease_details_df  – parsed Lease Details DataFrame
    contract_df       – parsed Contract Level Detail DataFrame
    unit_type_map     – dict  { floor_plan_code: { ... } } from Takeover Guide
                         (used only for the Unmapped Floor Plan flag)
    all_units_df      – parsed Final All Unit DataFrame
                         (source of per-unit market rents for the rent comparison)

All column name constants at the top of each function match the headers your
parser already normalises to (strip/upper).  Adjust if your normalisation differs.
"""

import pandas as pd
import streamlit as st


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 1 — ISSUE FLAG HELPERS
# Each returns a boolean Series aligned to rent_roll_df.index.
# ──────────────────────────────────────────────────────────────────────────────

def _flag_missing_email(rent_roll_df: pd.DataFrame,
                        contract_df: pd.DataFrame,
                        birthdays_df: pd.DataFrame | None = None) -> pd.Series:
    """True where no email was found from any fallback source."""
    # Build a unit→email lookup from Contract Level Detail
    cld_email = (
        contract_df
        .assign(_unit=contract_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4))
        .groupby("_unit")["E-MAIL"]
        .first()
        .str.strip()
    )
    if birthdays_df is not None:
        bd_email = (
            birthdays_df
            .assign(_unit=birthdays_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4))
            .groupby("_unit")["E-MAIL"]
            .first()
            .str.strip()
        )
        cld_email = cld_email.combine_first(bd_email)

    units = rent_roll_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4)
    found_email = units.map(cld_email).fillna("").str.strip()
    return found_email == ""


def _flag_missing_phone(lease_details_df: pd.DataFrame,
                        rent_roll_df: pd.DataFrame) -> pd.Series:
    """True where ALL three phone slots are empty for a resident."""
    ld = (
        lease_details_df
        .assign(_unit=lease_details_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4))
        .groupby("_unit")[["CELL PHONE", "HOME PHONE", "WORK PHONE"]]
        .first()
    )
    units = rent_roll_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4)

    def _empty(col):
        return units.map(ld[col].str.strip() if col in ld else pd.Series(dtype=str)).fillna("") == ""

    return _empty("CELL PHONE") & _empty("HOME PHONE") & _empty("WORK PHONE")


def _flag_missing_sign_date(lease_details_df: pd.DataFrame,
                             rent_roll_df: pd.DataFrame) -> pd.Series:
    """True where Lease Signed Date is missing or null."""
    ld = (
        lease_details_df
        .assign(_unit=lease_details_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4))
        .groupby("_unit")["LEASE SIGNED DATE"]
        .first()
    )
    units = rent_roll_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4)
    sign_date = units.map(ld).fillna("")
    return sign_date.astype(str).str.strip().isin(["", "nan", "NaT", "None"])


def _flag_unmapped_floor_plan(rent_roll_df: pd.DataFrame,
                               unit_type_map: dict) -> pd.Series:
    """True where the floor plan code has no Yardi mapping."""
    return ~rent_roll_df["UNIT TYPE"].str.strip().isin(unit_type_map.keys())


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 2 — RENT DISCREPANCY HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _build_rent_comparison(rent_roll_df: pd.DataFrame,
                            all_units_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame with one row per resident that has a rent discrepancy.

    Source of truth for market rent is the Final All Unit report (identified by
    "ALL UNITS" + "Amt/SQFT" in the file header), NOT the Takeover Guide.
    This gives a per-unit market rent rather than a per-floor-plan average.

    Expected All Units columns (after strip/upper normalisation):
        BLDG/UNIT  – unit code
        AMT/SQFT   – the market rent column header anchor; actual rent is in
                     the column your parser maps to "MARKET RENT" or "MKT RENT".
                     Adjust the constant ALLUNITS_RENT_COL below if your parser
                     names it differently.

    Columns returned:
        Unit        – zero-padded unit code
        Name        – resident name
        Status      – occupancy status
        Floor Plan  – OneSite unit type code
        Unit Rent   – market rent from All Units report for that unit
        Lease Rent  – from Rent Roll "LEASE RENT" column
        Difference  – Lease Rent − Unit Rent  (negative = under market)
        % Diff      – rounded to 1 dp
    """
    # ── Column name your parser uses for market rent in the All Units report ──
    # Common values: "MARKET RENT", "MKT RENT", "RENT/UNIT", "AMT"
    # Change this constant if your normalisation uses a different name.
    ALLUNITS_RENT_COL = "MARKET RENT"

    # Build a unit → market rent lookup from the All Units report
    au = all_units_df.copy()
    au["_unit"] = au["BLDG/UNIT"].astype(str).str.strip().str.zfill(4)
    au["_unit_rent"] = pd.to_numeric(
        au[ALLUNITS_RENT_COL].astype(str).str.replace(r"[$,]", "", regex=True).str.strip(),
        errors="coerce"
    )
    unit_rent_map = au.dropna(subset=["_unit_rent"]).set_index("_unit")["_unit_rent"]

    # Join market rent onto the rent roll by unit code
    df = rent_roll_df.copy()
    df["_unit"] = df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4)
    df["_unit_rent"] = df["_unit"].map(unit_rent_map)

    # Parse lease rent from the rent roll
    df["_lease_rent"] = pd.to_numeric(
        df["LEASE RENT"].astype(str).str.replace(r"[$,]", "", regex=True).str.strip(),
        errors="coerce"
    )

    # Keep only rows where both values are present AND differ (tolerance: $0.01)
    cmp = df.dropna(subset=["_unit_rent", "_lease_rent"]).copy()
    cmp["_diff"] = cmp["_lease_rent"] - cmp["_unit_rent"]
    cmp = cmp[cmp["_diff"].abs() >= 0.01].copy()

    if cmp.empty:
        return pd.DataFrame()

    cmp["% Diff"] = ((cmp["_diff"] / cmp["_unit_rent"]) * 100).round(1)

    result = cmp.rename(columns={
        "_unit":      "Unit",
        "NAME":       "Name",
        "STATUS":     "Status",
        "UNIT TYPE":  "Floor Plan",
        "_unit_rent": "Unit Rent",
        "_lease_rent":"Lease Rent",
        "_diff":      "Difference",
    })[["Unit", "Name", "Status", "Floor Plan",
        "Unit Rent", "Lease Rent", "Difference", "% Diff"]]

    return result.sort_values("Difference")


def _flag_rent_discrepancy(rent_roll_df: pd.DataFrame,
                            all_units_df: pd.DataFrame) -> pd.Series:
    """True where the resident's lease rent differs from the All Units unit rent."""
    cmp = _build_rent_comparison(rent_roll_df, all_units_df)
    if cmp.empty:
        return pd.Series(False, index=rent_roll_df.index)
    flagged_units = set(cmp["Unit"].tolist())
    return rent_roll_df["BLDG/UNIT"].astype(str).str.strip().str.zfill(4).isin(flagged_units)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 3 — COMBINED FLAGGED TENANT TABLE
# ──────────────────────────────────────────────────────────────────────────────

def _build_flagged_tenant_table(
    rent_roll_df: pd.DataFrame,
    lease_details_df: pd.DataFrame,
    contract_df: pd.DataFrame,
    unit_type_map: dict,
    all_units_df: pd.DataFrame,
    birthdays_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Returns a display-ready DataFrame containing only tenants with at least one
    data-quality issue, with colour-coded issue columns.

    Issue columns (✓ / ✗):
        Email      – missing from all fallback sources
        Phone      – all three phone slots empty
        Sign Date  – lease signed date missing
        Floor Plan – not in Yardi unit type map
        Rent       – lease rent ≠ unit rent from All Units report
    """
    df = rent_roll_df.copy()

    # Compute all flags
    flags = pd.DataFrame({
        "Email":      _flag_missing_email(df, contract_df, birthdays_df),
        "Phone":      _flag_missing_phone(lease_details_df, df),
        "Sign Date":  _flag_missing_sign_date(lease_details_df, df),
        "Floor Plan": _flag_unmapped_floor_plan(df, unit_type_map),
        "Rent":       _flag_rent_discrepancy(df, all_units_df),
    }, index=df.index)

    # Keep rows with ANY flag
    has_issue = flags.any(axis=1)
    df_issues = df[has_issue].copy()
    flags_issues = flags[has_issue]

    if df_issues.empty:
        return pd.DataFrame()

    # Build the display table
    display = pd.DataFrame({
        "Unit":       df_issues["BLDG/UNIT"].astype(str).str.strip().str.zfill(4),
        "Name":       df_issues["NAME"].str.strip(),
        "Status":     df_issues["STATUS"].str.strip(),
        "Floor Plan": df_issues["UNIT TYPE"].str.strip(),
        "Lease Rent": pd.to_numeric(
            df_issues["LEASE RENT"].astype(str).str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        ),
        # Issue columns — ✗ for flagged, empty string for clean
        "⚑ Email":      flags_issues["Email"].map({True: "✗ Missing", False: ""}),
        "⚑ Phone":      flags_issues["Phone"].map({True: "✗ Missing", False: ""}),
        "⚑ Sign Date":  flags_issues["Sign Date"].map({True: "✗ Missing", False: ""}),
        "⚑ Floor Plan": flags_issues["Floor Plan"].map({True: "✗ Unmapped", False: ""}),
        "⚑ Rent":       flags_issues["Rent"].map({True: "✗ Discrepancy", False: ""}),
    })

    return display.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 4 — STREAMLIT RENDER FUNCTION  (call this from your app)
# ──────────────────────────────────────────────────────────────────────────────

def render_validation_panel(
    rent_roll_df: pd.DataFrame,
    lease_details_df: pd.DataFrame,
    contract_df: pd.DataFrame,
    unit_type_map: dict,
    all_units_df: pd.DataFrame,
    birthdays_df: pd.DataFrame | None = None,
) -> None:
    """
    Renders the full Step 2 data-quality validation panel inside Streamlit.

    Parameters
    ----------
    rent_roll_df      : Final Rent Roll Detail DataFrame
    lease_details_df  : Lease Details DataFrame
    contract_df       : Contract Level Detail DataFrame
    unit_type_map     : dict { floor_plan_code: { ... } } from Takeover Guide
                        (used only for the Unmapped Floor Plan flag)
    all_units_df      : Final All Unit DataFrame  ← source of unit market rents
    birthdays_df      : Resident Birthdays DataFrame (optional email fallback)

    Includes:
      1. Summary badge row  (count of each issue type)
      2. Flagged tenants table  (only tenants with issues)
      3. Rent discrepancy detail table  (residents where lease rent ≠ unit rent)
    """

    st.subheader("🔍 Data Quality — Issues Requiring Review")
    st.caption(
        "Only tenants with at least one data quality issue are shown. "
        "Use the Validation Report workbook for the complete roster."
    )

    # ── 4.1  Build flagged table ──────────────────────────────────────────────
    flagged_df = _build_flagged_tenant_table(
        rent_roll_df, lease_details_df, contract_df,
        unit_type_map, all_units_df, birthdays_df
    )
    rent_cmp_df = _build_rent_comparison(rent_roll_df, all_units_df)

    total_tenants = len(rent_roll_df)

    # Issue counts for summary badges
    cnt_email      = int((flagged_df["⚑ Email"]      == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_phone      = int((flagged_df["⚑ Phone"]      == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_sign       = int((flagged_df["⚑ Sign Date"]  == "✗ Missing"      ).sum()) if not flagged_df.empty else 0
    cnt_fp         = int((flagged_df["⚑ Floor Plan"] == "✗ Unmapped"     ).sum()) if not flagged_df.empty else 0
    cnt_rent       = int((flagged_df["⚑ Rent"]       == "✗ Discrepancy"  ).sum()) if not flagged_df.empty else 0
    cnt_flagged    = len(flagged_df)

    # ── 4.2  Summary badges ───────────────────────────────────────────────────
    cols = st.columns(6)
    badge_style = "font-size:1.5rem; font-weight:700;"

    with cols[0]:
        colour = "#d32f2f" if cnt_flagged > 0 else "#388e3c"
        st.markdown(
            f"<p style='{badge_style} color:{colour}'>{cnt_flagged}</p>"
            f"<p style='font-size:0.78rem; color:#555;'>of {total_tenants} tenants flagged</p>",
            unsafe_allow_html=True
        )
    _badge(cols[1], cnt_email,   "Missing Email")
    _badge(cols[2], cnt_phone,   "Missing Phone")
    _badge(cols[3], cnt_sign,    "Missing Sign Date")
    _badge(cols[4], cnt_fp,      "Unmapped Floor Plan")
    _badge(cols[5], cnt_rent,    "Rent Discrepancy")

    st.divider()

    # ── 4.3  Flagged tenants table ────────────────────────────────────────────
    if flagged_df.empty:
        st.success("✅ No data quality issues found across all tenant records.")
    else:
        issue_cols = ["⚑ Email", "⚑ Phone", "⚑ Sign Date", "⚑ Floor Plan", "⚑ Rent"]
        
        # Filter controls
        with st.expander("Filter flagged tenants", expanded=False):
            filter_cols = st.multiselect(
                "Show only tenants with these specific issues:",
                options=issue_cols,
                default=[],
                help="Leave blank to show all flagged tenants."
            )

        filtered = flagged_df.copy()
        if filter_cols:
            mask = pd.Series(False, index=filtered.index)
            issue_value_map = {
                "⚑ Email":      "✗ Missing",
                "⚑ Phone":      "✗ Missing",
                "⚑ Sign Date":  "✗ Missing",
                "⚑ Floor Plan": "✗ Unmapped",
                "⚑ Rent":       "✗ Discrepancy",
            }
            for fc in filter_cols:
                mask |= (filtered[fc] == issue_value_map[fc])
            filtered = filtered[mask]

        st.caption(f"Showing **{len(filtered)}** flagged tenant(s).")

        st.dataframe(
            filtered.style
                .applymap(_red_if_flagged, subset=issue_cols)
                .format({"Lease Rent": "${:,.2f}"}, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ── 4.4  Rent discrepancy detail table ────────────────────────────────────
    st.subheader("💲 Resident Rent vs. Unit Rent Comparison")
    st.caption(
        "Compares each resident's **Lease Rent** (Rent Roll) against the "
        "**Unit Rent** for their specific unit (Final All Unit report). "
        "Units not present in the All Units report are excluded."
    )

    if rent_cmp_df.empty:
        st.success("✅ All resident lease rents match the unit rents in the All Units report.")
    else:
        over  = (rent_cmp_df["Difference"] > 0).sum()
        under = (rent_cmp_df["Difference"] < 0).sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Discrepancies", len(rent_cmp_df))
        c2.metric("Above Unit Rent",  over,  delta=f"+{over}",  delta_color="off")
        c3.metric("Below Unit Rent",  under, delta=f"-{under}", delta_color="inverse")

        st.dataframe(
            rent_cmp_df.style
                .applymap(_rent_diff_colour, subset=["Difference"])
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
                "- **Negative difference** — tenant is paying *below* the unit's market "
                "rent from the All Units report. Verify whether a concession, employee "
                "discount, or model-unit rate is intentional.\n"
                "- **Positive difference** — tenant is paying *above* the unit's market "
                "rent. Confirm the All Units report rent for this unit is correct before import.\n"
                "- Discrepancies here will be visible in Yardi after import. "
                "Resolve intentional variances with the appropriate charge code "
                "(e.g. `concmgr`, `emplcred`) before finalising the ETL."
            )


# ──────────────────────────────────────────────────────────────────────────────
# PRIVATE STYLING HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _badge(col, count: int, label: str) -> None:
    colour = "#d32f2f" if count > 0 else "#388e3c"
    col.markdown(
        f"<p style='font-size:1.5rem; font-weight:700; color:{colour}'>{count}</p>"
        f"<p style='font-size:0.78rem; color:#555;'>{label}</p>",
        unsafe_allow_html=True,
    )


def _red_if_flagged(val: str) -> str:
    """Pandas Styler applymap — highlight issue cells red."""
    if val and val.startswith("✗"):
        return "color: #c62828; font-weight: 600; background-color: #ffebee;"
    return ""


def _rent_diff_colour(val: float) -> str:
    """Pandas Styler applymap — colour rent differences."""
    if pd.isna(val):
        return ""
    if val < 0:
        return "color: #c62828; font-weight: 600; background-color: #ffebee;"  # red = below market
    if val > 0:
        return "color: #e65100; font-weight: 600; background-color: #fff3e0;"  # amber = above market
    return ""
