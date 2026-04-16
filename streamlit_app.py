"""
RPM Living · Yardi ETL Converter  —  Two-Phase Workflow
Phase 1: Resident + property files (Tenant_Code blank), then upload Yardi export for Phase 2
Phase 2: Upload Yardi-assigned tcodes → generate tcode-dependent files
"""
import streamlit as st
import pandas as pd
import tempfile, os, zipfile, shutil, re
from io import BytesIO

st.set_page_config(
    page_title="RPM Living · ETL Converter",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');
html,[class*="css"]{font-family:'DM Sans',sans-serif}
h1,h2,h3{font-family:'Syne',sans-serif!important}
code,.stCode{font-family:'IBM Plex Mono',monospace!important}
.stProgress>div>div>div>div{background:linear-gradient(90deg,#3b6bff,#8b5cf6)!important}
.stTabs [data-baseweb="tab-list"]{gap:8px;border-bottom:2px solid #1e2840}
.stTabs [data-baseweb="tab"]{border-radius:6px 6px 0 0;padding:8px 20px;font-family:'Syne',sans-serif;font-weight:600;font-size:13px}
[data-testid="metric-container"]{background:#111520;border:1px solid #1e2840;border-radius:10px;padding:16px}
[data-testid="stFileUploaderDropzone"]{border:1.5px dashed #1e2840!important;border-radius:10px!important;background:#111520!important}
.stDownloadButton button{background:linear-gradient(135deg,#10d9a0,#059669)!important;color:white!important;font-family:'Syne',sans-serif!important;font-weight:700!important;border:none!important;border-radius:8px!important;padding:12px 28px!important}
.stButton>button[kind="primary"]{background:linear-gradient(135deg,#3b6bff,#8b5cf6)!important;color:white!important;font-family:'Syne',sans-serif!important;font-weight:700!important;border:none!important;border-radius:8px!important}
#MainMenu,footer,header{visibility:hidden}
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
DEFAULTS = {
    "step": 1,            # 1=upload 2=validate 3=phase1_done 4=tcode_upload 5=phase2_done
    "mappings": None,
    "vdata": None,
    "amenity_overrides": {},
    "property_code": "",
    "include_former_bal": True,
    "base_dir": None,
    "tenants": None,      # serialised from Phase 1, used in Phase 2
    "phase1_zip": None,
    "phase2_zip": None,
    "tmp_dirs": [],
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Helpers ───────────────────────────────────────────────────────────────────
def badge(txt, colour):
    colours = {"green":("rgba(16,217,160,.15)","#10d9a0"),"yellow":("rgba(240,192,64,.15)","#f0c040"),
                "red":("rgba(255,77,94,.15)","#ff4d5e"),"blue":("rgba(59,107,255,.15)","#3b6bff")}
    bg,fg = colours.get(colour,colours["blue"])
    return f'<span style="background:{bg};color:{fg};font-family:IBM Plex Mono;font-size:11px;padding:2px 9px;border-radius:10px">{txt}</span>'

def reset():
    for k, v in DEFAULTS.items():
        st.session_state[k] = v if k != "tmp_dirs" else []
    st.rerun()

def set_step(n):
    st.session_state.step = n

# ── Top bar ───────────────────────────────────────────────────────────────────
col_logo, col_title, col_steps = st.columns([1, 5, 4])
with col_logo:
    st.markdown('<div style="background:linear-gradient(135deg,#3b6bff,#8b5cf6);width:40px;height:40px;border-radius:9px;display:flex;align-items:center;justify-content:center;font-family:Syne;font-weight:800;font-size:18px;color:white;margin-top:4px">R</div>', unsafe_allow_html=True)
with col_title:
    st.markdown('<h2 style="margin:0;font-size:18px;letter-spacing:-.3px">RPM Living · Yardi ETL Converter</h2>', unsafe_allow_html=True)
    st.markdown('<p style="margin:0;color:#5a6a88;font-size:11px;font-family:IBM Plex Mono">OneSite → Yardi Voyager  ·  Two-Phase Workflow</p>', unsafe_allow_html=True)
with col_steps:
    STEPS = ["Upload","Validate","Phase 1","Map Tcodes","Phase 2"]
    cur = st.session_state.step
    html = ""
    for i, s in enumerate(STEPS, 1):
        done   = i < cur
        active = i == cur
        bg  = "#10d9a0" if done else ("#3b6bff" if active else "#1e2840")
        fg  = "white"   if (done or active) else "#5a6a88"
        html += f'<span style="background:{bg};color:{fg};padding:4px 11px;border-radius:20px;font-size:11px;font-family:Syne;font-weight:600;margin-left:3px">{i} {s}</span>'
    st.markdown(f'<div style="margin-top:9px;text-align:right">{html}</div>', unsafe_allow_html=True)
st.markdown('<hr style="border:none;border-top:1px solid #1e2840;margin:14px 0">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════
#  SHARED — build validation data
# ══════════════════════════════════════════════════════════
def _build_vdata(base, mappings, property_code):
    from converter import (
        load_rent_roll, load_lease_details, load_contract_details,
        load_all_residents, load_unit_setup, load_rentable_items,
        load_insurance, load_prospects, load_all_unit, load_birthdays,
        build_tenant_base, load_former_records, CHARGE_CODE_MAP, _find_file,
    )
    import pandas as _pd

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

    # Eviction promotion
    try:
        ld_ev = _pd.read_excel(_find_file(base,"Lease Details .xlsx"), header=8)
        evict_ids = set(
            ld_ev[ld_ev["Eviction proceedings started"]=="Yes"]["Household ID/ Resh ID"]
            .dropna().apply(lambda x: int(float(x)) if str(x).replace(".0","").isdigit() else 0)
        )
        for rid,t in tenants.items():
            if rid in evict_ids and t["status"]==0: t["status"]=10
    except Exception: pass

    for rid,t in load_former_records(base).items():
        if rid not in tenants: tenants[rid]=t

    t_vals = list(tenants.values())
    curr   = [t for t in t_vals if t["status"]==0]
    notice = [t for t in t_vals if t["status"]==4]
    future = [t for t in t_vals if t["status"]==6]
    evict  = [t for t in t_vals if t["status"]==10]
    fbal   = [t for t in t_vals if t["status"]==5]
    no_em  = [t for t in t_vals if not t["email"]]
    no_ph  = [t for t in t_vals if not t["phone1"] and not t["phone2"]]
    no_sg  = [t for t in t_vals if not t["lease_sign"]]

    fps = sorted(rr["Floorplan"].dropna().unique())
    ut_rows, am_rows, ch_rows, tn_rows, unmapped_ut = [], [], [], [], []

    for fp in fps:
        ut  = mappings["unit_type_map"].get(fp)
        sub = rr[rr["Floorplan"]==fp]
        occ = len(sub[sub["Unit/Lease Status"]=="Occupied"])
        tot = len(sub["unit_code"].unique())
        if ut:
            ut_rows.append({"OneSite Code":fp,"Yardi Code":ut["yardi_code"],
                "Description":ut.get("desc",""),"Beds":ut["beds"],"Baths":int(ut["baths"]),
                "SQFT":ut["sqft"],"Market Rent":ut["rent"],
                "Total Units":tot,"Occupied":occ,"Status":"✅ Mapped"})
        else:
            rec = {"OneSite Code":fp,"Yardi Code":"UNMAPPED","Description":"",
                   "Beds":"","Baths":"","SQFT":"","Market Rent":"",
                   "Total Units":tot,"Occupied":occ,"Status":"❌ Missing"}
            ut_rows.append(rec); unmapped_ut.append(rec)

    for name in sorted(unit_setup["Unit amenity Name"].dropna().unique()):
        mapped = mappings["amenity_map"].get(name)
        cnt = int((unit_setup["Unit amenity Name"]==name).sum())
        if mapped:
            desc,code,amt = mapped
            am_rows.append({"OneSite Name":name,"RPM Description":desc,"Yardi Code":code,
                "Monthly Amt ($)":amt,"Units":cnt,"Status":"✅ Mapped"})
        else:
            am_rows.append({"OneSite Name":name,"RPM Description":name,"Yardi Code":name[:15],
                "Monthly Amt ($)":0,"Units":cnt,"Status":"⚠️ Auto"})

    for col,ycode in CHARGE_CODE_MAP.items():
        if col in rr.columns:
            active = int((rr[col].fillna(0)!=0).sum())
            total_amt = float(rr[col].fillna(0).abs().sum())
            ch_rows.append({"OneSite Column":col,"Yardi Code":ycode,
                "Active Leases":active,"Monthly Total ($)":round(total_amt,2),
                "Status":"✅ Active" if active>0 else "— Inactive"})

    status_lbl = {0:"🟢 Current",4:"🟡 Notice",6:"🔵 Future",10:"🔴 Eviction",5:"🟠 Former/Bal"}

    # Preview rows (60) for the Streamlit table
    tn_rows = []
    for t in sorted(t_vals, key=lambda x: x["unit_code"] or "9999")[:60]:
        tn_rows.append({"Unit":t["unit_code"],"Name":f"{t['last_name']}, {t['first_name']}",
            "Status":status_lbl.get(t["status"],"?"),
            "Lease From":t["lease_from"] or "–","Lease To":t["lease_to"] or "–",
            "Rent":f"${t['rent']:,}" if t["rent"] else "–",
            "Email":"✅" if t["email"] else "❌",
            "Phone":"✅" if (t["phone1"] or t["phone2"]) else "❌"})

    # Full tenant list for the Excel workbook (all tenants, sorted by unit)
    tn_full = []
    for t in sorted(t_vals, key=lambda x: x["unit_code"] or "9999"):
        tn_full.append({
            "Unit":           t["unit_code"],
            "Name":           f"{t['last_name']}, {t['first_name']}",
            "Status":         status_lbl.get(t["status"],"?"),
            "status_code":    t["status"],
            "has_email":      bool(t["email"]),
            "has_phone":      bool(t["phone1"] or t["phone2"]),
            "has_sign_date":  bool(t.get("lease_sign")),
            "floorplan":      t.get("floorplan", ""),
            "Lease From":     t["lease_from"] or "",
            "Lease To":       t["lease_to"]   or "",
            "Rent":           t["rent"] or 0,
            "Email":          "✅" if t["email"] else "❌",
            "Phone":          "✅" if (t["phone1"] or t["phone2"]) else "❌",
        })

    # ── Rent discrepancy: resident lease rent vs unit market rent (All Units) ──
    unit_market_rent = {}
    for _, row in all_unit.iterrows():
        uc = row.get("unit_code", "")
        mkt_raw = str(row.get("Market Rent", "0")).replace(",", "").replace("$", "").strip()
        try:
            mkt = float(mkt_raw)
        except (ValueError, TypeError):
            mkt = 0.0
        if uc and mkt > 0:
            unit_market_rent[uc] = mkt

    rent_disc = []
    for t in sorted(t_vals, key=lambda x: x["unit_code"] or "9999"):
        uc         = t.get("unit_code", "")
        lease_rent = float(t.get("rent") or 0)
        unit_rent  = unit_market_rent.get(uc)
        if unit_rent and abs(lease_rent - unit_rent) >= 0.01:
            diff = round(lease_rent - unit_rent, 2)
            pct  = round((diff / unit_rent) * 100, 1) if unit_rent else 0.0
            rent_disc.append({
                "Unit":       uc,
                "Name":       f"{t['last_name']}, {t['first_name']}",
                "Status":     status_lbl.get(t["status"], "?"),
                "Floor Plan": t.get("floorplan", ""),
                "Unit Rent":  unit_rent,
                "Lease Rent": lease_rent,
                "Difference": diff,
                "% Diff":     pct,
            })
    rent_disc.sort(key=lambda x: x["Difference"])

    # Warnings list
    warnings = []
    if unmapped_ut:
        warnings.append({"level":"ERROR","msg":"Floor plan(s) not in Takeover Guide",
            "items":[r["OneSite Code"] for r in unmapped_ut]})
    if no_em:
        warnings.append({"level":"WARN","msg":f"{len(no_em)} tenant(s) missing email address",
            "items":[f"{t['unit_code']} {t['last_name']}" for t in no_em[:20]]})
    if no_ph:
        warnings.append({"level":"WARN","msg":f"{len(no_ph)} tenant(s) missing phone number",
            "items":[f"{t['unit_code']} {t['last_name']}" for t in no_ph[:20]]})
    if no_sg:
        warnings.append({"level":"INFO","msg":f"{len(no_sg)} tenant(s) missing lease sign date",
            "items":[f"{t['unit_code']} {t['last_name']}" for t in no_sg[:20]]})
    auto_amenities = [r for r in am_rows if "Auto" in r["Status"]]
    if auto_amenities:
        warnings.append({"level":"WARN","msg":f"{len(auto_amenities)} amenity/amenities not in Takeover Guide (using source name as code)",
            "items":[r["OneSite Name"] for r in auto_amenities]})

    assigned  = rent_items[rent_items["status"].isin(["In Use","Leased","NTV"])]
    available = rent_items[rent_items["status"]=="Unassigned"]

    return {
        "summary":{"current":len(curr),"notice":len(notice),"future":len(future),
                   "eviction":len(evict),"former_bal":len(fbal),
                   "total_units":len(rr["unit_code"].unique()),
                   "garages_assigned":len(assigned),"garages_available":len(available),
                   "ri_policies":len(ins_df),"prospects":len(pros_df)},
        "quality":{"no_email":no_em,"no_phone":no_ph,"no_sign":no_sg},
        "unit_types":ut_rows,"amenities":am_rows,"charges":ch_rows,
        "tenants":tn_rows,"tenants_full":tn_full,
        "unmapped_ut":unmapped_ut,"warnings":warnings,
        "rent_discrepancies":rent_disc,
    }

#  STEP 1 — UPLOAD
# ══════════════════════════════════════════════════════════
if st.session_state.step == 1:
    st.markdown("### Upload Source Files")
    st.markdown('<p style="color:#5a6a88;margin-bottom:22px">Upload the OneSite export files and the RPM Takeover Guide. Files are identified by their report title content — filenames and folder structure do not matter.</p>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📋 RPM Takeover Guide**")
        st.caption("Excel workbook with all property-specific mappings")
        rpm_file = st.file_uploader(
            "Takeover Guide", type=["xlsx","xls"],
            key="rpm_xlsx", label_visibility="collapsed"
        )
    with col2:
        st.markdown("**📂 OneSite Export Files**")
        st.caption("Select all OneSite report files at once — any format, any filename")
        src_files = st.file_uploader(
            "OneSite Reports", type=["xls","xlsx"],
            key="src_files", label_visibility="collapsed",
            accept_multiple_files=True
        )

    if rpm_file and src_files:
        st.caption(f"✅ {len(src_files)} source file(s) selected")
        st.markdown("---")
        col_pc, col_bal = st.columns([1, 2])
        with col_pc:
            prop_code = st.text_input(
                "Yardi Property Code (numeric)",
                placeholder="e.g. 13400",
                help="Numeric property code assigned in Yardi Voyager — the only value not in the Takeover Guide.",
                value=st.session_state.property_code,
            )
        with col_bal:
            st.markdown("**Past Resident Balances**")
            bal_choice = st.radio(
                "Include former residents with outstanding balance?",
                ["Include past resident balances", "Exclude past resident balances"],
                index=0 if st.session_state.include_former_bal else 1,
                help="**Include** — exports ETL_ResTenants_FormerBal (status 5).\n\n**Exclude** — only current/notice/future/eviction tenants are exported.",
            )
            st.session_state.include_former_bal = (bal_choice == "Include past resident balances")

        if st.button("⬆ Parse & Validate Mappings", type="primary", disabled=not (prop_code or "").strip()):
            if not prop_code.strip().isdigit():
                st.error("Property code must be numeric (e.g. 13400)")
            else:
                with st.spinner("Identifying reports and loading source data..."):
                    try:
                        tmp_g = tempfile.mkdtemp()
                        tmp_s = tempfile.mkdtemp()
                        st.session_state.tmp_dirs.extend([tmp_g, tmp_s])

                        # Save Takeover Guide
                        guide_path = os.path.join(tmp_g, "guide.xlsx")
                        with open(guide_path, "wb") as f: f.write(rpm_file.read())

                        # Save all source files into a flat temp directory
                        # Filenames are preserved so the report-title scanner can open each one
                        for uf in src_files:
                            dest = os.path.join(tmp_s, uf.name)
                            with open(dest, "wb") as f: f.write(uf.read())

                        base = tmp_s + "/"

                        import sys; sys.path.insert(0, os.path.dirname(__file__))
                        from converter import load_takeover_guide, _scan_report_titles, _FILE_CACHE
                        _FILE_CACHE.clear()   # force fresh scan for new upload
                        matched = _scan_report_titles(base)

                        # Confirm the Rent Roll (minimum required file) was found
                        if "Final Rent Roll Detail with Lease Charges.xls" not in matched:
                            st.error(
                                "❌ Could not identify the Rent Roll report among the uploaded files. "
                                "Please ensure 'Final Rent Roll Detail with Lease Charges' is included."
                            )
                            st.stop()

                        mappings = load_takeover_guide(guide_path)
                        vdata    = _build_vdata(base, mappings, prop_code.strip())
                        st.session_state.update({
                            "mappings": mappings, "vdata": vdata,
                            "property_code": prop_code.strip(), "base_dir": base,
                        })
                        set_step(2); st.rerun()
                    except Exception as e:
                        import traceback
                        st.error(f"❌ {e}"); st.code(traceback.format_exc())

    elif not rpm_file and src_files:
        st.warning("⚠️ Please also upload the RPM Takeover Guide — it contains all unit type, amenity, and property mappings.")
    elif rpm_file and not src_files:
        st.info("ℹ️ Upload the OneSite export files to continue.")

    # Workflow explainer
    st.markdown("---")
    st.markdown("#### Two-Phase Workflow")
    c1,c2,c3,c4,c5 = st.columns(5)
    for col, n, title, desc in [
        (c1,"1","Upload & Validate","Upload files and review all mapping tables"),
        (c2,"2","Phase 1 Download","Download resident ETL files (Tenant_Code blank) + all property files"),
        (c3,"3","Import to Yardi","Import Phase 1 into Yardi → Yardi assigns tcodes → fill in the template"),
        (c4,"4","Upload Tcodes","Upload the completed tcode mapping file"),
        (c5,"5","Phase 2 Download","Download tcode-dependent files with real Yardi tcodes"),
    ]:
        col.markdown(f'<div style="background:#111520;border:1px solid #1e2840;border-radius:8px;padding:14px;text-align:center"><div style="background:#3b6bff;color:white;width:28px;height:28px;border-radius:50%;font-family:Syne;font-weight:700;font-size:14px;display:flex;align-items:center;justify-content:center;margin:0 auto 8px">{n}</div><div style="font-family:Syne;font-weight:600;font-size:12px;margin-bottom:6px">{title}</div><div style="font-size:11px;color:#5a6a88;line-height:1.5">{desc}</div></div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
#  STEP 2 — VALIDATE
# ══════════════════════════════════════════════════════════
elif st.session_state.step == 2:
    v = st.session_state.vdata
    m = st.session_state.mappings
    pc= st.session_state.property_code
    S = v["summary"]
    include_former = st.session_state.include_former_bal

    col_hdr, col_run = st.columns([6,2])
    with col_hdr:
        st.markdown(f"### {m['prop_name']}  ·  #{pc}")
        st.caption(f"{m['address']}  ·  {m['city']}, {m['state']} {m['zipcode']}  ·  Prefix: {m['prop_prefix']}")
    with col_run:
        run_p1 = st.button("⚡ Generate Phase 1 Files", type="primary", use_container_width=True)

    # Balance filter banner
    if include_former:
        st.info(f"✅ **Past resident balances: Included** — {S['former_bal']} former residents with open balance will be in ETL_ResTenants_FormerBal.", icon="✅")
    else:
        st.warning(f"🚫 **Past resident balances: Excluded** — {S['former_bal']} former residents will NOT be exported. Change on the Upload screen.", icon="⚠️")

    if run_p1:
        if v["unmapped_ut"]:
            st.error(f"❌ {len(v['unmapped_ut'])} floor plan(s) unmapped — fix in Takeover Guide first.")
        else:
            set_step(3); st.rerun()

    # Metrics
    cols8 = st.columns(8)
    for col, lbl, val in zip(cols8, ["Current","On Notice","Future","Eviction",
        "Former w/Bal" if include_former else "Fmr/Bal (excl)","Total Units","RI Policies","Prospects"],
        [S["current"],S["notice"],S["future"],S["eviction"],
         S["former_bal"] if include_former else f"{S['former_bal']} (excl)",
         S["total_units"],S["ri_policies"],S["prospects"]]):
        col.metric(lbl, val)

    # Quality
    Q = v["quality"]
    n_email = len(Q["no_email"])
    n_phone = len(Q["no_phone"])
    n_sign  = len(Q["no_sign"])
    qcols = st.columns(3)
    for col, lbl, count in zip(qcols,
        ["Email", "Phone", "Lease Sign Date"],
        [n_email, n_phone, n_sign],
    ):
        if count == 0:
            col.success(f"✅ {lbl}: all present")
        else:
            col.warning(f"⚠️ {lbl}: {count} missing")

    st.markdown("")
    tab_ut, tab_am, tab_ch, tab_tn, tab_out = st.tabs([
        f"🏠 Unit Types ({len(v['unit_types'])})",
        f"✨ Amenities ({len(v['amenities'])})",
        f"💳 Charges ({len(v['charges'])})",
        f"👥 Tenants ({len(v['tenants'])}+)",
        "📁 Output Plan",
    ])
    with tab_ut:
        st.caption("Floor plan → Yardi unit type mappings from the Takeover Guide.")
        if v["unmapped_ut"]: st.error(f"❌ {len(v['unmapped_ut'])} unmapped floor plans")
        else: st.success("✅ All floor plans mapped")
        st.dataframe(pd.DataFrame(v["unit_types"]), use_container_width=True, hide_index=True)

    with tab_am:
        st.info("💡 Edit any **Monthly Amt** before generating — changes apply to all unit amenity records.", icon="✏️")
        am_df = pd.DataFrame([{**r, "Monthly Amt ($)": st.session_state.amenity_overrides.get(r["Yardi Code"], r["Monthly Amt ($)"])} for r in v["amenities"]])
        edited = st.data_editor(am_df, use_container_width=True, hide_index=True,
            column_config={"Monthly Amt ($)": st.column_config.NumberColumn(min_value=-9999, max_value=9999, step=1),
                           **{c: st.column_config.TextColumn(disabled=True) for c in ["OneSite Name","RPM Description","Yardi Code","Units","Status"]}},
            key="amenity_editor")
        for _, row in edited.iterrows():
            orig = next((r["Monthly Amt ($)"] for r in v["amenities"] if r["Yardi Code"]==row["Yardi Code"]), 0)
            if row["Monthly Amt ($)"] != orig:
                st.session_state.amenity_overrides[row["Yardi Code"]] = row["Monthly Amt ($)"]

    with tab_ch:
        st.dataframe(pd.DataFrame(v["charges"]), use_container_width=True, hide_index=True)

    with tab_tn:
        from validation_panel import render_validation_panel
        render_validation_panel(v)

    with tab_out:
        st.markdown("#### Phase 1 — Resident & Property Files *(Tenant_Code blank)*")
        p1 = [
            ("ETL_ResTenants","Current residents (status=0) — **Tenant_Code blank**",S["current"]),
            ("ETL_ResTenants_Notice","On-notice residents (status=4) — **Tenant_Code blank**",S["notice"]),
            ("ETL_ResTenants_Future","Future/applicant (status=6) — **Tenant_Code blank**",S["future"]),
            ("ETL_ResTenants_Eviction","Eviction proceedings (status=10) — **Tenant_Code blank**",S["eviction"]),
        ]
        if include_former: p1.append(("ETL_ResTenants_FormerBal","Former w/ balance (status=5) — **Tenant_Code blank**",S["former_bal"]))
        p1 += [
            ("ETL_ResRentableItemsTypes","Rentable item types",len(m["rentable_types"]) or 1),
            ("ETL_ResRentableItems","Individual rentable items","—"),
            ("ETL_ResUnitTypes",f"Unit types ({len(m['unit_type_map'])} types)",len(m["unit_type_map"])),
            ("ETL_CommUnits","All units with addresses",S["total_units"]),
            ("ETL_ResPropertyAmenities","Property amenities",len(m["amenity_map"])),
            ("ETL_ResUnitAmenities","Per-unit amenity charges","—"),
            ("ETL_ResProspects","Guest cards & prospects",S["prospects"]),

        ]
        st.dataframe(pd.DataFrame(p1,columns=["File","Contents","Est. Rows"]), use_container_width=True, hide_index=True)

        st.markdown("#### Phase 2 — Tcode-Dependent Files *(after Yardi assigns codes)*")
        p2 = [
            ("ETL_ResRoommates","Co-tenants & roommates — Tenant_Code from Yardi","—"),
            ("ETL_RIPolicies","Renters insurance policies — Tenant_Code from Yardi",S["ri_policies"]),
            ("ETL_ResLeaseCharges","Recurring lease charges — Tenant_Code from Yardi","—"),
            ("ETL_ResManageRentableItems","Rentable item assignments — Tenant_Code from Yardi","—"),
            ("ETL_leasebut_demo","Demographic data — demo_tcode from Yardi","—"),
        ]
        st.dataframe(pd.DataFrame(p2,columns=["File","Contents","Est. Rows"]), use_container_width=True, hide_index=True)

    st.markdown("---")
    bc, _, rc = st.columns([2,4,2])
    with bc:
        if st.button("↩ Start Over"): reset()
    with rc:
        if st.button("⚡ Generate Phase 1 Files", type="primary", use_container_width=True, key="run2"):
            if not v["unmapped_ut"]: set_step(3); st.rerun()


# ══════════════════════════════════════════════════════════
#  STEP 3 — PHASE 1 GENERATE & DOWNLOAD
# ══════════════════════════════════════════════════════════
elif st.session_state.step == 3:
    m    = st.session_state.mappings
    pc   = st.session_state.property_code
    base = st.session_state.base_dir

    # Apply amenity overrides
    if st.session_state.amenity_overrides:
        for pn, (desc, code, _) in m["amenity_map"].items():
            if code in st.session_state.amenity_overrides:
                m["amenity_map"][pn] = (desc, code, st.session_state.amenity_overrides[code])

    if st.session_state.phase1_zip is None:
        st.markdown("### ⚡ Generating Phase 1 Files...")
        pbar  = st.progress(0); log_el = st.empty(); msgs = []; total = 13

        def cb(msg):
            msgs.append(msg)
            done = sum(1 for x in msgs if "✅" in x)
            pbar.progress(min(5+int(done/total*88),95))
            log_el.code("\n".join(msgs[-16:]))

        try:
            from converter import run_phase1, build_validation_workbook
            tmp_out = tempfile.mkdtemp()
            st.session_state.tmp_dirs.append(tmp_out)
            gen1, zip1, tenants = run_phase1(
                base, tmp_out, m, pc,
                include_former_bal=st.session_state.include_former_bal,
                progress_cb=cb,
            )
            pbar.progress(100); log_el.code("\n".join(msgs))
            with open(zip1,"rb") as f: st.session_state.phase1_zip = f.read()
            st.session_state.tenants = tenants

            # Build validation workbook from vdata already in session
            cb("📊 Building validation workbook...")
            val_path = os.path.join(tmp_out, f"{pc}_Validation_Report.xlsx")
            build_validation_workbook(st.session_state.vdata, m, pc, tenants, val_path)
            with open(val_path, "rb") as f:
                st.session_state.validation_xlsx = f.read()
            cb("   ✅ Validation_Report.xlsx")

            st.rerun()
        except Exception as e:
            import traceback
            st.error(f"❌ {e}"); st.code(traceback.format_exc())
    else:
        st.markdown("### ✅ Phase 1 Complete")
        st.success("Resident and property files generated. Tenant_Code columns are **blank** — Yardi will assign them on import.")
        st.markdown("")

        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            st.markdown("**📦 Phase 1 ETL Files**")
            st.caption("Resident files (blank Tenant_Code) + all property files — import these into Yardi first")
            st.download_button(
                "⬇ Download Phase 1 ETL Package",
                data=st.session_state.phase1_zip,
                file_name=f"{pc}_Phase1_ETL.zip",
                mime="application/zip",
                use_container_width=True,
            )
        with col_dl2:
            st.markdown("**📊 Validation Report**")
            st.caption("Full tenant roster, mapping tables, data quality flags, and warnings — review before importing")
            val_data = st.session_state.get("validation_xlsx")
            if val_data:
                st.download_button(
                    "⬇ Download Validation_Report.xlsx",
                    data=val_data,
                    file_name=f"{pc}_Validation_Report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

        st.markdown("---")
        st.info(
            "**Next steps:**\n"
            "1. Review the Validation Report for any issues before importing\n"
            "2. Import the resident ETL files from the Phase 1 zip into Yardi\n"
            "3. Yardi assigns a unique Tenant_Code to each resident upon import\n"
            "4. Export the resident records from Yardi (the file will have the Tenant_Code column populated)\n"
            "5. Return here and upload that exported file to generate the Phase 2 files",
            icon="ℹ️",
        )
        st.markdown("")
        if st.button("➡ Continue to Upload Yardi Resident Export", type="primary"):
            set_step(4); st.rerun()
        if st.button("↩ Start Over"): reset()


# ══════════════════════════════════════════════════════════
#  STEP 4 — UPLOAD TCODE MAPPING
# ══════════════════════════════════════════════════════════
elif st.session_state.step == 4:
    st.markdown("### Upload Yardi Resident Export")
    st.markdown('<p style="color:#5a6a88">Upload the resident ETL files exported from Yardi after Phase 1 import. Yardi populates the <strong>Tenant_Code</strong> column. Upload all relevant variant files — Current, Notice, Future, Eviction, FormerBal — for full coverage.</p>', unsafe_allow_html=True)

    mapping_files = st.file_uploader(
        "ETL_ResTenants files from Yardi",
        type=["xlsx","xls"],
        key="tcode_map_file",
        label_visibility="collapsed",
        accept_multiple_files=True,
        help="Upload one or more: ETL_ResTenants, ETL_ResTenants_Notice, _Future, _Eviction, _FormerBal. Tcodes are merged from all files.",
    )
    st.caption("You can upload multiple variant files at once — all Tenant_Codes are merged automatically.")

    if mapping_files:
        try:
            from converter import load_tcode_mapping
            tmp_map = tempfile.mkdtemp()
            st.session_state.tmp_dirs.append(tmp_map)

            tenants   = st.session_state.tenants or {}
            tcode_map = {}
            file_results = []

            for mf in mapping_files:
                map_path = os.path.join(tmp_map, mf.name)
                with open(map_path,"wb") as f: f.write(mf.read())
                partial   = load_tcode_mapping(map_path, tenants)
                new_count = sum(1 for rid in partial if rid not in tcode_map)
                tcode_map.update(partial)
                file_results.append({"File": mf.name, "Tcodes in file": len(partial), "New tcodes added": new_count})

            if len(mapping_files) > 1:
                st.dataframe(pd.DataFrame(file_results), use_container_width=True, hide_index=True)

            filled  = len(tcode_map)
            total_t = len(tenants)
            missing = total_t - filled

            col_m1, col_m2, col_m3 = st.columns(3)
            col_m1.metric("Tcodes Matched", filled)
            col_m2.metric("Total Tenants",  total_t)
            col_m3.metric("Unmatched",       missing,
                          delta=None if missing==0 else f"{missing} will have blank tcode",
                          delta_color="off" if missing>0 else "normal")

            if missing == 0:
                st.success("✅ All tenants matched — full tcode coverage.")
            else:
                st.warning(
                    f"⚠️ {missing} tenant(s) not yet matched. "
                    "Upload additional variant files (Notice, Future, Eviction, FormerBal) to fill gaps. "
                    "Unmatched tenants will have a blank Tenant_Code in Phase 2 files."
                )

            preview = []
            for rid, tc in list(tcode_map.items())[:10]:
                t = tenants.get(rid, {})
                preview.append({
                    "Unit": t.get("unit_code","?"),
                    "Name": f"{t.get('last_name','')}, {t.get('first_name','')}",
                    "Status": {0:"Current",4:"Notice",6:"Future",10:"Eviction",5:"Former/Bal"}.get(t.get("status",0),"?"),
                    "Yardi Tenant_Code": tc,
                })
            if preview:
                st.caption("Sample matches:")
                st.dataframe(pd.DataFrame(preview), use_container_width=True, hide_index=True)

            st.markdown("")
            if st.button("⚡ Generate Phase 2 Files", type="primary"):
                st.session_state.tcode_map = tcode_map
                set_step(5); st.rerun()

        except Exception as e:
            import traceback
            st.error(f"❌ Could not read file(s): {e}")
            st.code(traceback.format_exc())

    st.markdown("")
    if st.button("↩ Back to Phase 1 Download"): set_step(3); st.rerun()


# ══════════════════════════════════════════════════════════
#  STEP 5 — PHASE 2 GENERATE & DOWNLOAD
# ══════════════════════════════════════════════════════════
elif st.session_state.step == 5:
    m        = st.session_state.mappings
    pc       = st.session_state.property_code
    base     = st.session_state.base_dir
    tenants  = st.session_state.tenants
    tcode_map= st.session_state.get("tcode_map", {})

    if st.session_state.phase2_zip is None:
        st.markdown("### ⚡ Generating Phase 2 Files...")
        pbar  = st.progress(0); log_el = st.empty(); msgs = []; total = 5

        def cb(msg):
            msgs.append(msg)
            done = sum(1 for x in msgs if "✅" in x)
            pbar.progress(min(5+int(done/total*88),95))
            log_el.code("\n".join(msgs[-10:]))

        try:
            from converter import run_phase2
            tmp_out = tempfile.mkdtemp()
            st.session_state.tmp_dirs.append(tmp_out)
            gen2, zip2 = run_phase2(base, tmp_out, m, pc, tenants, tcode_map, progress_cb=cb)
            pbar.progress(100); log_el.code("\n".join(msgs))
            with open(zip2,"rb") as f: st.session_state.phase2_zip = f.read()
            st.rerun()
        except Exception as e:
            import traceback
            st.error(f"❌ {e}"); st.code(traceback.format_exc())
    else:
        st.markdown("### ✅ Phase 2 Complete")
        st.success("All tcode-dependent ETL files generated with real Yardi tenant codes.")
        st.markdown("")

        st.download_button(
            "⬇ Download Phase 2 ETL Package (.zip)",
            data=st.session_state.phase2_zip,
            file_name=f"{pc}_Phase2_ETL.zip",
            mime="application/zip",
            use_container_width=True,
        )

        st.markdown("---")
        st.markdown("""
**Phase 2 package contains:**
| File | Tcode Column |
|---|---|
| ETL_ResRoommates | Tenant_Code (primary tenant) |
| ETL_RIPolicies | Tenant_Code, Tenant_Code1 |
| ETL_ResLeaseCharges | Tenant_Code |
| ETL_ResManageRentableItems | Tenant_Code |
| ETL_leasebut_demo | demo_tcode |
""")
        st.markdown("")
        if st.button("↩ Convert Another Property"): reset()


