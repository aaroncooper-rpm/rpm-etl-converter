"""
RPM Living · Yardi ETL Converter
Streamlit application — deployable to Streamlit Community Cloud (free)
"""
import streamlit as st
import pandas as pd
import tempfile, os, zipfile, shutil, re
from io import BytesIO
from pathlib import Path

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RPM Living · ETL Converter",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Inject custom CSS ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1,h2,h3 { font-family: 'Syne', sans-serif !important; }
code, .stCode { font-family: 'IBM Plex Mono', monospace !important; }

/* Progress bar accent */
.stProgress > div > div > div > div { background: linear-gradient(90deg,#3b6bff,#8b5cf6) !important; }

/* Success/warning/error boxes */
.stAlert { border-radius: 8px !important; }

/* Dataframe headers */
.stDataFrame thead { background-color: #1F4E79 !important; }

/* Tab styling */
.stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom: 2px solid #1e2840; }
.stTabs [data-baseweb="tab"] { border-radius: 6px 6px 0 0; padding: 8px 20px; font-family:'Syne',sans-serif; font-weight:600; font-size:13px; }

/* Metric cards */
[data-testid="metric-container"] { background: #111520; border: 1px solid #1e2840; border-radius: 10px; padding: 16px; }

/* Input fields */
.stTextInput input, .stNumberInput input { font-family: 'IBM Plex Mono', monospace !important; }

/* File uploader */
[data-testid="stFileUploaderDropzone"] { border: 1.5px dashed #1e2840 !important; border-radius: 10px !important; background: #111520 !important; }

/* Download button */
.stDownloadButton button { background: linear-gradient(135deg,#10d9a0,#059669) !important; color: white !important; font-family:'Syne',sans-serif !important; font-weight:700 !important; border:none !important; border-radius:8px !important; padding:12px 28px !important; }

/* Primary button */
.stButton > button[kind="primary"] { background: linear-gradient(135deg,#3b6bff,#8b5cf6) !important; color:white !important; font-family:'Syne',sans-serif !important; font-weight:700 !important; border:none !important; border-radius:8px !important; }

/* Hide streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def badge(text, colour):
    colours = {
        "green":  ("rgba(16,217,160,.15)", "#10d9a0"),
        "yellow": ("rgba(240,192,64,.15)", "#f0c040"),
        "red":    ("rgba(255,77,94,.15)",  "#ff4d5e"),
        "blue":   ("rgba(59,107,255,.15)", "#3b6bff"),
        "grey":   ("rgba(90,106,136,.15)", "#5a6a88"),
    }
    bg, fg = colours.get(colour, colours["grey"])
    return f'<span style="background:{bg};color:{fg};font-family:IBM Plex Mono;font-size:11px;padding:2px 9px;border-radius:10px;font-weight:500">{text}</span>'


def build_validation_data(base, mappings, property_code):
    """Load source files and return a rich dict for display."""
    from converter import (
        load_rent_roll, load_lease_details, load_contract_details,
        load_all_residents, load_unit_setup, load_rentable_items,
        load_insurance, load_prospects, load_all_unit, load_birthdays,
        build_tenant_base, CHARGE_CODE_MAP, STATUS_MAP
    )

    rr         = load_rent_roll(base)
    ld_idx     = load_lease_details(base)
    cld        = load_contract_details(base)
    all_res    = load_all_residents(base)
    unit_setup = load_unit_setup(base)
    rent_items = load_rentable_items(base)
    ins_df     = load_insurance(base)
    pros_df    = load_prospects(base)
    all_unit   = load_all_unit(base)
    bdays      = load_birthdays(base)
    tenants    = build_tenant_base(rr, ld_idx, cld, bdays, mappings, property_code)

    t_vals = list(tenants.values())
    curr   = [t for t in t_vals if t["status"] == 0]
    notice = [t for t in t_vals if t["status"] == 4]
    future = [t for t in t_vals if t["status"] == 6]

    no_email = [t for t in t_vals if not t["email"]]
    no_phone = [t for t in t_vals if not t["phone1"] and not t["phone2"]]
    no_sign  = [t for t in t_vals if not t["lease_sign"]]

    # Unit type mapping table
    fps = sorted(rr["Floorplan"].dropna().unique())
    ut_rows = []
    for fp in fps:
        ut  = mappings["unit_type_map"].get(fp)
        sub = rr[rr["Floorplan"] == fp]
        occ = len(sub[sub["Unit/Lease Status"] == "Occupied"])
        tot = len(sub["unit_code"].unique())
        if ut:
            ut_rows.append({"OneSite Code": fp, "Yardi Code": ut["yardi_code"],
                "Description": ut.get("desc",""), "Beds": ut["beds"],
                "Baths": ut["baths"], "SQFT": ut["sqft"], "Market Rent": ut["rent"],
                "Total Units": tot, "Occupied": occ, "Status": "✅ Mapped"})
        else:
            ut_rows.append({"OneSite Code": fp, "Yardi Code": "UNMAPPED",
                "Description": "", "Beds": "", "Baths": "", "SQFT": "", "Market Rent": "",
                "Total Units": tot, "Occupied": occ, "Status": "❌ Missing"})

    # Amenity mapping table
    am_rows = []
    for name in sorted(unit_setup["Unit amenity Name"].dropna().unique()):
        mapped = mappings["amenity_map"].get(name)
        cnt = int((unit_setup["Unit amenity Name"] == name).sum())
        if mapped:
            desc, code, amt = mapped
            am_rows.append({"OneSite Name": name, "RPM Description": desc,
                "Yardi Code": code, "Monthly Amt ($)": amt, "Units": cnt, "Status": "✅ Mapped"})
        else:
            am_rows.append({"OneSite Name": name, "RPM Description": name,
                "Yardi Code": name[:15], "Monthly Amt ($)": 0, "Units": cnt, "Status": "⚠️ Auto"})

    # Charge code table
    ch_rows = []
    for col, ycode in CHARGE_CODE_MAP.items():
        if col in rr.columns:
            active    = int((rr[col].fillna(0) != 0).sum())
            total_amt = float(rr[col].fillna(0).abs().sum())
            ch_rows.append({"OneSite Column": col, "Yardi Code": ycode,
                "Active Leases": active, "Monthly Total ($)": round(total_amt, 2),
                "Status": "✅ Active" if active > 0 else "— Inactive"})

    # Garage summary
    assigned  = rent_items[rent_items["status"].isin(["In Use","Leased","NTV"])]
    available = rent_items[rent_items["status"] == "Unassigned"]

    # Tenant preview
    tn_rows = []
    for t in sorted(t_vals, key=lambda x: x["unit_code"] or "9999")[:60]:
        status_label = {0:"🟢 Current",4:"🟡 Notice",6:"🔵 Future"}.get(t["status"],"?")
        tn_rows.append({
            "Unit": t["unit_code"], "Tenant Code": t["tenant_code"],
            "Name": f"{t['last_name']}, {t['first_name']}",
            "Status": status_label,
            "Lease From": t["lease_from"] or "–", "Lease To": t["lease_to"] or "–",
            "Rent": f"${t['rent']:,}" if t["rent"] else "–",
            "Email": "✅" if t["email"] else "❌",
            "Phone": "✅" if (t["phone1"] or t["phone2"]) else "❌",
        })

    return {
        "summary": {
            "current": len(curr), "notice": len(notice), "future": len(future),
            "total_units": len(rr["unit_code"].unique()),
            "garages_assigned": len(assigned), "garages_available": len(available),
            "ri_policies": len(ins_df), "prospects": len(pros_df),
        },
        "quality": {
            "no_email": no_email, "no_phone": no_phone, "no_sign": no_sign,
        },
        "unmapped_ut": [r for r in ut_rows if "Missing" in r["Status"]],
        "unmapped_am": [r for r in am_rows if "Auto" in r["Status"]],
        "unit_types":  ut_rows,
        "amenities":   am_rows,
        "charges":     ch_rows,
        "tenants":     tn_rows,
        "rr": rr, "ld_idx": ld_idx, "cld": cld, "all_res": all_res,
        "unit_setup": unit_setup, "rent_items": rent_items,
        "ins_df": ins_df, "pros_df": pros_df, "all_unit": all_unit, "bdays": bdays,
    }


# ── App state ─────────────────────────────────────────────────────────────────
if "step" not in st.session_state:
    st.session_state.step = 1        # 1=upload, 2=validate, 3=done
if "mappings" not in st.session_state:
    st.session_state.mappings = None
if "vdata" not in st.session_state:
    st.session_state.vdata = None
if "amenity_overrides" not in st.session_state:
    st.session_state.amenity_overrides = {}
if "property_code" not in st.session_state:
    st.session_state.property_code = ""
if "base_dir" not in st.session_state:
    st.session_state.base_dir = None
if "output_zip" not in st.session_state:
    st.session_state.output_zip = None
if "tmp_dirs" not in st.session_state:
    st.session_state.tmp_dirs = []


# ── Header ────────────────────────────────────────────────────────────────────
col_logo, col_title, col_step = st.columns([1, 6, 3])
with col_logo:
    st.markdown('<div style="background:linear-gradient(135deg,#3b6bff,#8b5cf6);width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-family:Syne;font-weight:800;font-size:22px;color:white;margin-top:4px">R</div>', unsafe_allow_html=True)
with col_title:
    st.markdown('<h2 style="margin:0;padding:0;font-size:20px;letter-spacing:-.3px">RPM Living · Yardi ETL Converter</h2>', unsafe_allow_html=True)
    st.markdown('<p style="margin:0;color:#5a6a88;font-size:12px;font-family:IBM Plex Mono">OneSite → Yardi Voyager  ·  Any RPM Property</p>', unsafe_allow_html=True)
with col_step:
    steps = ["Upload", "Validate & Map", "Download"]
    step_html = ""
    for i, s in enumerate(steps, 1):
        active = i == st.session_state.step
        done   = i <  st.session_state.step
        colour = "#10d9a0" if done else ("#3b6bff" if active else "#2a3550")
        step_html += f'<span style="background:{colour};color:{"white" if active or done else "#5a6a88"};padding:4px 12px;border-radius:20px;font-size:11px;font-family:Syne;font-weight:600;margin-left:4px">{i} {s}</span>'
    st.markdown(f'<div style="margin-top:10px;text-align:right">{step_html}</div>', unsafe_allow_html=True)

st.markdown('<hr style="border:none;border-top:1px solid #1e2840;margin:16px 0">', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════
#  STEP 1 — UPLOAD
# ══════════════════════════════════════════════════════
if st.session_state.step == 1:
    st.markdown("### Upload Source Files")
    st.markdown('<p style="color:#5a6a88;margin-bottom:24px">Both files are required. The Takeover Guide provides all property-specific mappings — no configuration is hardcoded.</p>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📦 Conversion Final Reporting**")
        st.caption("The zip export from OneSite containing all reports")
        src_file = st.file_uploader("Source ZIP", type=["zip"], key="src_zip", label_visibility="collapsed")
    with col2:
        st.markdown("**📋 RPM Takeover Guide**")
        st.caption("The Excel workbook containing all property mappings")
        rpm_file = st.file_uploader("Takeover Guide", type=["xlsx","xls"], key="rpm_xlsx", label_visibility="collapsed")

    if src_file and rpm_file:
        st.markdown("---")
        prop_code = st.text_input(
            "Yardi Property Code (numeric)",
            placeholder="e.g. 13400",
            help="The numeric property code assigned in Yardi Voyager. This is the only field not in the Takeover Guide.",
            value=st.session_state.property_code
        )

        if st.button("⬆ Parse & Validate Mappings", type="primary", disabled=not prop_code.strip()):
            if not prop_code.strip().isdigit():
                st.error("Property code must be numeric (e.g. 13400)")
            else:
                with st.spinner("Parsing Takeover Guide and loading source data..."):
                    try:
                        # Save files to temp dirs
                        tmp_guide = tempfile.mkdtemp()
                        tmp_src   = tempfile.mkdtemp()
                        st.session_state.tmp_dirs.extend([tmp_guide, tmp_src])

                        guide_path = os.path.join(tmp_guide, "guide.xlsx")
                        with open(guide_path, "wb") as f:
                            f.write(rpm_file.read())

                        zip_path = os.path.join(tmp_src, "source.zip")
                        with open(zip_path, "wb") as f:
                            f.write(src_file.read())

                        # Extract source
                        extract_dir = os.path.join(tmp_src, "extracted")
                        with zipfile.ZipFile(zip_path) as zf:
                            zf.extractall(extract_dir)

                        base = None
                        for root, _, files_list in os.walk(extract_dir):
                            if "Final Rent Roll Detail with Lease Charges.xls" in files_list:
                                base = root + "/"; break
                        if not base:
                            st.error("❌ Could not find source files. Ensure the zip contains the '2. Final Reporting' folder.")
                            st.stop()

                        # Load mappings
                        import sys
                        sys.path.insert(0, os.path.dirname(__file__))
                        from converter import load_takeover_guide
                        mappings = load_takeover_guide(guide_path)

                        # Build validation
                        vdata = build_validation_data(base, mappings, prop_code.strip())

                        st.session_state.mappings = mappings
                        st.session_state.vdata    = vdata
                        st.session_state.property_code = prop_code.strip()
                        st.session_state.base_dir = base
                        st.session_state.step     = 2
                        st.rerun()
                    except Exception as e:
                        import traceback
                        st.error(f"❌ {e}")
                        st.code(traceback.format_exc())
    elif src_file and not rpm_file:
        st.warning("⚠️ The RPM Takeover Guide is required — it contains all unit type, amenity, and rentable item mappings for this property.")
    elif rpm_file and not src_file:
        st.info("ℹ️ Upload the ConversionFinalReporting.zip to continue.")


# ══════════════════════════════════════════════════════
#  STEP 2 — VALIDATE
# ══════════════════════════════════════════════════════
elif st.session_state.step == 2:
    v  = st.session_state.vdata
    m  = st.session_state.mappings
    pc = st.session_state.property_code
    S  = v["summary"]
    Q  = v["quality"]

    # Header row
    col_info, col_run = st.columns([6, 2])
    with col_info:
        st.markdown(f"### {m['prop_name']}  ·  #{pc}")
        st.caption(f"{m['address']}  ·  {m['city']}, {m['state']} {m['zipcode']}  ·  Prefix: {m['prop_prefix']}")
    with col_run:
        run_clicked = st.button("⚡ Run Conversion", type="primary", use_container_width=True)
    
    if run_clicked:
        if v["unmapped_ut"]:
            st.error(f"❌ Cannot convert: {len(v['unmapped_ut'])} floor plan(s) are not mapped in the Takeover Guide.")
        else:
            st.session_state.step = 3
            st.rerun()

    # ── Metric cards ──────────────────────────────────────────────────────
    c1,c2,c3,c4,c5,c6,c7,c8 = st.columns(8)
    for col, label, value, delta_color in [
        (c1, "Current",     S["current"],           "normal"),
        (c2, "On Notice",   S["notice"],            "inverse"),
        (c3, "Future",      S["future"],            "normal"),
        (c4, "Total Units", S["total_units"],        "off"),
        (c5, "Garages Leased",   S["garages_assigned"],  "normal"),
        (c6, "Garages Free",     S["garages_available"], "normal"),
        (c7, "RI Policies",      S["ri_policies"],        "normal"),
        (c8, "Prospects",        S["prospects"],          "normal"),
    ]:
        col.metric(label, value)

    st.markdown("")

    # ── Quality flags ──────────────────────────────────────────────────────
    qcols = st.columns(4)
    qdata = [
        ("Email Coverage",  len(Q["no_email"]) == 0, f"{len(Q['no_email'])} missing"),
        ("Phone Coverage",  len(Q["no_phone"]) == 0, f"{len(Q['no_phone'])} missing"),
        ("Lease Sign Date", len(Q["no_sign"]) == 0,  f"{len(Q['no_sign'])} missing"),
        ("Unit Type Mapping", len(v["unmapped_ut"]) == 0, f"{len(v['unmapped_ut'])} unmapped"),
    ]
    for col, (label, ok, detail) in zip(qcols, qdata):
        if ok:
            col.success(f"✅ {label}")
        else:
            col.warning(f"⚠️ {label}: {detail}")

    # Warn about missing emails/phones
    for issue, items in [("email", Q["no_email"]), ("phone", Q["no_phone"])]:
        if items:
            with st.expander(f"⚠️ {len(items)} tenant(s) missing {issue} — click to see", expanded=False):
                st.dataframe(
                    pd.DataFrame([{"Unit": t["unit_code"], "Name": f"{t['last_name']}, {t['first_name']}"} for t in items]),
                    use_container_width=True, hide_index=True
                )

    st.markdown("---")

    # ── Tabbed validation panels ───────────────────────────────────────────
    tab_ut, tab_am, tab_ch, tab_tn, tab_out = st.tabs([
        f"🏠 Unit Types ({len(v['unit_types'])})",
        f"✨ Amenities ({len(v['amenities'])})",
        f"💳 Charge Codes ({len(v['charges'])})",
        f"👥 Tenants ({len(v['tenants'])}+)",
        "📁 Output Files",
    ])

    # ─ Unit Types ─────────────────────────────────────────────────────────
    with tab_ut:
        st.caption("All OneSite floor plans must map to a Yardi unit type code. These mappings come from the **Unit Type** sheet of the Takeover Guide.")
        if v["unmapped_ut"]:
            st.error(f"❌ {len(v['unmapped_ut'])} floor plan(s) are not in the Takeover Guide — add them before converting.")
        else:
            st.success("✅ All floor plans are mapped")
        df_ut = pd.DataFrame(v["unit_types"])
        st.dataframe(df_ut, use_container_width=True, hide_index=True,
                     column_config={"Market Rent": st.column_config.NumberColumn("Market Rent ($)", format="$%d")})

    # ─ Amenities ──────────────────────────────────────────────────────────
    with tab_am:
        st.caption("Amenity names from OneSite map to RPM codes and monthly charges via the **Property Amenities** sheet of the Takeover Guide.")
        st.info("💡 You can edit any **Monthly Amt** before converting — changes apply to all generated unit amenity records.", icon="✏️")

        if v["unmapped_am"]:
            st.warning(f"⚠️ {len(v['unmapped_am'])} amenity/amenities not in the Takeover Guide — will use source name as code.")

        # Editable dataframe
        am_df = pd.DataFrame([{
            "OneSite Name": r["OneSite Name"],
            "RPM Description": r["RPM Description"],
            "Yardi Code": r["Yardi Code"],
            "Monthly Amt ($)": st.session_state.amenity_overrides.get(r["Yardi Code"], r["Monthly Amt ($)"]),
            "Units": r["Units"],
            "Status": r["Status"],
        } for r in v["amenities"]])

        edited = st.data_editor(
            am_df,
            use_container_width=True, hide_index=True,
            column_config={
                "Monthly Amt ($)": st.column_config.NumberColumn("Monthly Amt ($)", min_value=-9999, max_value=9999, step=1),
                "OneSite Name": st.column_config.TextColumn(disabled=True),
                "RPM Description": st.column_config.TextColumn(disabled=True),
                "Yardi Code": st.column_config.TextColumn(disabled=True),
                "Units": st.column_config.NumberColumn(disabled=True),
                "Status": st.column_config.TextColumn(disabled=True),
            },
            key="amenity_editor"
        )
        # Store overrides
        orig_amts = {r["Yardi Code"]: r["Monthly Amt ($)"] for r in v["amenities"]}
        for _, row in edited.iterrows():
            code = row["Yardi Code"]; orig = orig_amts.get(code, 0)
            if row["Monthly Amt ($)"] != orig:
                st.session_state.amenity_overrides[code] = row["Monthly Amt ($)"]

        if st.session_state.amenity_overrides:
            st.caption(f"✏️ {len(st.session_state.amenity_overrides)} amount(s) edited — changes will be applied at conversion time.")

    # ─ Charge Codes ───────────────────────────────────────────────────────
    with tab_ch:
        st.caption("Charge columns from the OneSite Rent Roll map to Yardi transaction codes for `ETL_ResLeaseCharges`. These are standard OneSite column names, not property-specific.")
        df_ch = pd.DataFrame(v["charges"])
        st.dataframe(df_ch, use_container_width=True, hide_index=True,
                     column_config={"Monthly Total ($)": st.column_config.NumberColumn("Monthly Total ($)", format="$%.2f")})
        active = sum(1 for r in v["charges"] if "Active" in r["Status"])
        st.caption(f"{active} active charge types · {len(v['charges'])-active} inactive (will be skipped)")

    # ─ Tenant Preview ─────────────────────────────────────────────────────
    with tab_tn:
        st.caption("First 60 active tenant records sorted by unit number. Use this to spot-check data quality before converting.")
        df_tn = pd.DataFrame(v["tenants"])
        st.dataframe(df_tn, use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("🟢 Current Residents", S["current"])
        c2.metric("🟡 On Notice", S["notice"])
        c3.metric("🔵 Future / Applicants", S["future"])

    # ─ Output Forecast ────────────────────────────────────────────────────
    with tab_out:
        st.caption("15 files will be generated from your source data.")
        FORECAST = [
            ("ETL_ResTenants",            "Current residents (status=0)",              S["current"]),
            ("ETL_ResTenants_Notice",     "On-notice residents (status=4)",             S["notice"]),
            ("ETL_ResTenants_Future",     "Future / applicant residents (status=6)",    S["future"]),
            ("ETL_ResRoommates",          "Co-occupants & roommates",                   "—"),
            ("ETL_ResRentableItemsTypes", "Rentable item type definitions",             len(m["rentable_types"]) or 1),
            ("ETL_ResRentableItems",      "Individual rentable items",                  "—"),
            ("ETL_RIPolicies",            "Renters insurance policies",                 S["ri_policies"]),
            ("ETL_ResLeaseCharges",       "Recurring lease charges (per tenant)",        "—"),
            ("ETL_ResManageRentableItems","Rentable item assignments",                  S["garages_assigned"]),
            ("ETL_leasebut_demo",         "Demographic & employment data",              "—"),
            ("ETL_ResProspects",          "Guest cards & prospect records",             S["prospects"]),
            ("ETL_ResUnitTypes",          f"Unit type definitions ({len(m['unit_type_map'])} types)", len(m["unit_type_map"])),
            ("ETL_CommUnits",             "All unit records with addresses",            S["total_units"]),
            ("ETL_ResPropertyAmenities",  f"Property-level amenities ({len(m['amenity_map'])} mapped)", len(m["amenity_map"])),
            ("ETL_ResUnitAmenities",      "Per-unit amenity charges",                  "—"),
        ]
        fc_df = pd.DataFrame(FORECAST, columns=["File", "Contents", "Est. Rows"])
        st.dataframe(fc_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    col_back, col_space, col_run2 = st.columns([2,4,2])
    with col_back:
        if st.button("↩ Start Over"):
            for k in ["step","mappings","vdata","amenity_overrides","property_code","base_dir","output_zip"]:
                st.session_state[k] = None if k != "step" else 1
                if k == "amenity_overrides": st.session_state[k] = {}
            st.rerun()
    with col_run2:
        if st.button("⚡ Run Conversion", type="primary", use_container_width=True, key="run2"):
            if not v["unmapped_ut"]:
                st.session_state.step = 3
                st.rerun()


# ══════════════════════════════════════════════════════
#  STEP 3 — CONVERT & DOWNLOAD
# ══════════════════════════════════════════════════════
elif st.session_state.step == 3:
    m  = st.session_state.mappings
    pc = st.session_state.property_code
    base = st.session_state.base_dir

    # Apply amenity amount overrides to mappings
    if st.session_state.amenity_overrides:
        for prior_name, (desc, code, amt) in m["amenity_map"].items():
            if code in st.session_state.amenity_overrides:
                m["amenity_map"][prior_name] = (desc, code, st.session_state.amenity_overrides[code])

    if st.session_state.output_zip is None:
        st.markdown("### ⚡ Converting...")
        pbar  = st.progress(0)
        log_el = st.empty()
        msgs  = []
        total = 15

        def cb(msg):
            msgs.append(msg)
            done = sum(1 for m_ in msgs if "✅" in m_)
            pbar.progress(min(5 + int(done / total * 90), 95))
            log_el.code("\n".join(msgs[-18:]))

        try:
            from converter import run_conversion
            tmp_out = tempfile.mkdtemp()
            st.session_state.tmp_dirs.append(tmp_out)

            generated, zip_path = run_conversion(base, tmp_out, m, pc, progress_cb=cb)

            pbar.progress(100)
            log_el.code("\n".join(msgs))

            with open(zip_path, "rb") as f:
                st.session_state.output_zip = f.read()

            st.session_state.step = 3   # stay on step 3 for download
            st.rerun()
        except Exception as e:
            import traceback
            st.error(f"❌ Conversion failed: {e}")
            st.code(traceback.format_exc())

    else:
        # Show download UI
        st.markdown("### ✅ Conversion Complete")
        st.success(f"**{m['prop_name']}** · Property Code {pc} · All 15 ETL files generated")

        st.markdown("")
        dl_col, info_col = st.columns([2, 3])
        with dl_col:
            ts = __import__("datetime").datetime.now().strftime("%y%m%d_%H%M%S")
            fname = f"{ts}_{pc}_ETL_Output.zip"
            st.download_button(
                label="⬇  Download ETL Package (.zip)",
                data=st.session_state.output_zip,
                file_name=fname,
                mime="application/zip",
                use_container_width=True,
            )
            st.markdown("")
            if st.button("↩ Convert Another Property", use_container_width=True):
                for k in ["step","mappings","vdata","amenity_overrides","property_code","base_dir","output_zip"]:
                    if k == "step": st.session_state[k] = 1
                    elif k == "amenity_overrides": st.session_state[k] = {}
                    else: st.session_state[k] = None
                st.rerun()

        with info_col:
            st.markdown("**Package contains:**")
            FILES = [
                ("ETL_ResTenants", "Current residents"),
                ("ETL_ResTenants_Notice", "On-notice residents"),
                ("ETL_ResTenants_Future", "Future residents"),
                ("ETL_ResRoommates", "Roommates & co-occupants"),
                ("ETL_ResRentableItemsTypes", "Rentable item types"),
                ("ETL_ResRentableItems", "Individual rentable items"),
                ("ETL_RIPolicies", "Renters insurance policies"),
                ("ETL_ResLeaseCharges", "Recurring lease charges"),
                ("ETL_ResManageRentableItems", "Rentable item assignments"),
                ("ETL_leasebut_demo", "Demographic data"),
                ("ETL_ResProspects", "Prospect / guest cards"),
                ("ETL_ResUnitTypes", "Unit type definitions"),
                ("ETL_CommUnits", "Unit records with addresses"),
                ("ETL_ResPropertyAmenities", "Property amenities"),
                ("ETL_ResUnitAmenities", "Per-unit amenity charges"),
            ]
            for i in range(0, len(FILES), 3):
                r = st.columns(3)
                for j, (fn, fd) in enumerate(FILES[i:i+3]):
                    r[j].caption(f"📄 **{fn}**  \n{fd}")
