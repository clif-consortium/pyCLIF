"""PDF and text report generator for DQA validation results."""

import csv
import hashlib
import json
import os
from collections import OrderedDict
from html import escape
from typing import Dict, Any, Optional, List
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Flowable
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from clifpy.utils.rule_codes import enrich_issue, truncate_comment, passing_finding


class YearlySparkBar(Flowable):
    """Mini bar chart showing per-year unique ID counts for a category value.

    Bars are colored blue when the value is present, red when absent.
    """

    def __init__(self, yearly_counts: Dict[int, int], width: float = 2.5 * inch,
                 height: float = 20):
        super().__init__()
        self.yearly_counts = yearly_counts
        self.width = width
        self.height = height

    _LABEL_HEIGHT = 8  # space reserved for year tick labels

    def wrap(self, availWidth, availHeight):
        return self.width, self.height + self._LABEL_HEIGHT

    def draw(self):
        if not self.yearly_counts:
            return
        years = sorted(self.yearly_counts.keys())
        n = len(years)
        if n == 0:
            return
        max_count = max(self.yearly_counts.values()) or 1
        gap = 0.5
        bar_w = max(1, (self.width - gap * (n - 1)) / n)
        present_color = colors.HexColor('#4A90D9')
        absent_color = colors.HexColor('#E74C3C')
        label_offset = self._LABEL_HEIGHT  # bars drawn above label area
        for i, year in enumerate(years):
            count = self.yearly_counts[year]
            x = i * (bar_w + gap)
            if count > 0:
                bar_h = max(2, (count / max_count) * self.height)
                self.canv.setFillColor(present_color)
            else:
                bar_h = self.height  # full height red bar for absent
                self.canv.setFillColor(absent_color)
            self.canv.rect(x, label_offset, bar_w, bar_h, stroke=0, fill=1)
        # Draw year tick labels for first and last bars
        self.canv.setFillColor(colors.HexColor('#666666'))
        self.canv.setFont('Helvetica', 5.5)
        first_x = 0
        self.canv.drawString(first_x, 0, str(years[0]))
        if n > 1:
            last_x = (n - 1) * (bar_w + gap)
            self.canv.drawString(last_x, 0, str(years[-1]))

DQA_CATEGORIES = ('conformance', 'completeness', 'plausibility')


def _make_error_id(issue: Dict[str, Any]) -> str:
    """Create a unique identifier for a DQA issue, matching feedback.create_error_id."""
    msg = issue.get('message', '')
    desc_hash = hashlib.md5(msg.encode()).hexdigest()[:8]
    category = issue.get('category', '')
    check_type = issue.get('check_type', 'unknown')
    prefix = f"{category}_{check_type}" if category else check_type
    prefix = prefix.replace(' ', '_').lower()
    return f"{prefix}_{desc_hash}"


def _collapse_info_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse INFO-severity rows sharing a rule_code into one row per group.

    Non-INFO rows (error/warning) pass through unchanged, preserving order.
    For each INFO group of size > 1, emit one merged row whose column_field
    is the comma-joined, deduped list of the originals' column_field values.
    Groups of size 1 pass through as-is.

    Warnings are NOT collapsed — per-value sparkline rows (e.g. P.6) need to
    stay distinct so each value's yearly-presence pattern is visible.
    """
    info_groups: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r.get('severity') != 'info':
            out.append(r)
            continue
        key = r.get('rule_code') or r.get('check_type') or ''
        info_groups.setdefault(key, []).append(r)

    for key, grp in info_groups.items():
        if len(grp) == 1:
            out.append(grp[0])
            continue
        # Dedupe column_field values (P.6 emits many INFO rows all tagged with
        # the same category column; without dedupe the merged row becomes
        # "organism_category, organism_category, ..." and overflows a PDF cell).
        seen: List[str] = []
        for r in grp:
            c = r.get('column_field', '')
            if c and c != 'NA' and c not in seen:
                seen.append(c)
        merged = dict(grp[0])
        joined = ', '.join(seen)
        if len(joined) > 200:
            joined = joined[:197] + '...'
        merged['column_field'] = joined if joined else merged.get('column_field', '')
        finding = passing_finding(merged.get('rule_code', ''))
        # Surface thresholds when the check carries them in details
        # (e.g., K.1 missingness, P.2 numeric_range_plausibility).
        thresh_suffix = _threshold_suffix(grp)
        if thresh_suffix:
            finding = f"{finding} {thresh_suffix}"
        merged['finding'] = finding
        merged['message'] = merged['finding']
        merged['details'] = {'count': len(grp), 'columns': seen}
        # Sum per-row atomic_count so checks like K.3 (where each column's
        # INFO row carries its own atomic_count) keep the right total.
        # For checks whose INFO rows are all atomic_count=1, this still
        # equals len(grp) — behavior-preserving.
        merged['atomic_count'] = sum(r.get('atomic_count', 1) for r in grp)
        out.append(merged)
    return out


def _threshold_suffix(rows: List[Dict[str, Any]]) -> str:
    """Return a "(err >X%, warn >Y%)" suffix when the check's rows carry
    error_threshold/warning_threshold in their details. Returns '' when
    no thresholds are recorded.
    """
    for r in rows:
        d = r.get('details') or {}
        err = d.get('error_threshold')
        warn = d.get('warning_threshold')
        if err is not None and warn is not None:
            return f"(err >{err}%, warn >{warn}%)"
        if err is not None:
            return f"(err >{err}%)"
        if warn is not None:
            return f"(warn >{warn}%)"
    return ''


def _reconcile_atomic_counts(
    rows: List[Dict[str, Any]],
    atomic_total: int,
    atomic_passed: int,
    category: str,
    check_type: str,
    check_key: Optional[str] = None,
) -> None:
    """Align per-row ``atomic_count`` with the check's ``atomic_total``.

    Strategy:
      * Leave error/warning row counts (set by ``enrich_issue`` heuristics)
        alone — they represent failing/flagged atoms.
      * If an INFO row exists (silent-pass summary), set its ``atomic_count``
        to the remaining atoms (``atomic_total - err/warn sum``).
      * Otherwise synthesize one INFO row so the passing atoms are visible
        and the per-row counts sum to ``atomic_total``.

    Mutates ``rows`` in place.
    """
    if atomic_total == 0:
        return

    err_warn_sum = sum(
        r.get('atomic_count', 1) for r in rows
        if r.get('severity') in ('error', 'warning')
    )
    remaining = max(0, atomic_total - err_warn_sum)

    info_rows = [r for r in rows if r.get('severity') == 'info']
    if info_rows:
        info = info_rows[0]
        info['atomic_count'] = remaining
        # Partial-pass rewrite: if error/warning rows exist for this same
        # check, an "All X" rollup contradicts them and should become
        # "Remaining X". But per-atom INFO rows that passed through
        # collapse unchanged (e.g. K.4's "All patient_id values in labs
        # exist in patient") carry specific detail — don't flatten those
        # into the generic partial text. Heuristic: only rewrite when the
        # current finding already matches passing_finding(rule_code),
        # which identifies the collapsed-aggregate rollup emitted by
        # _collapse_info_rows.
        partial = any(r.get('severity') in ('error', 'warning') for r in rows)
        if partial:
            rule_code = info.get('rule_code', '')
            if rule_code:
                generic = passing_finding(rule_code, partial=False)
                if info.get('finding') == generic:
                    info['finding'] = passing_finding(rule_code, partial=True)
                    info['message'] = info['finding']
        return

    if remaining > 0:
        # Gather distinct column_fields from existing err/warn rows so the
        # synthesized "passed atoms" row points at the same subject columns
        # (e.g. K.3 ADT: hospital_type, location_category, location_type).
        cols_seen: List[str] = []
        for r in rows:
            c = r.get('column_field')
            if not c or c == 'NA':
                continue
            for piece in c.split(', '):
                p = piece.strip()
                if p and p not in cols_seen:
                    cols_seen.append(p)

        # Partial-pass context: some atoms in this check already errored or
        # warned. The synth INFO row represents only the *remaining* passing
        # atoms, not every atom — so the finding says "Remaining X" rather
        # than "All X" to avoid contradicting the failing rows above it.
        partial = any(r.get('severity') in ('error', 'warning') for r in rows)

        synth = {
            'category': category,
            'check_type': check_type,
            'severity': 'info',
            'message': '',
            'details': {'count': remaining, 'columns_checked': cols_seen},
        }
        enriched = enrich_issue(synth, check_key=check_key)
        if enriched is not None:
            enriched['atomic_count'] = remaining
            enriched['finding'] = passing_finding(
                enriched.get('rule_code', ''), partial=partial,
            )
            enriched['message'] = enriched['finding']
            if cols_seen:
                joined = ', '.join(cols_seen)
                if len(joined) > 200:
                    joined = joined[:197] + '...'
                enriched['column_field'] = joined
            else:
                enriched['column_field'] = enriched.get('column_field') or 'NA'
            rows.append(enriched)


def collect_dqa_issues(validation_data: Dict[str, Any]):
    """Collect errors, warnings, and info messages from run_full_dqa output.

    Returns (category_scores, all_issues) where each issue is a dict with
    category, check_type, severity ('error'/'warning'/'info'), message, details,
    plus enriched fields: rule_code, rule_description, column_field.

    Scoring reads ``atomic_total``/``atomic_passed`` on each check's result.
    Both fields are **required** on every DQAResult producer — the check's
    "natural atomic unit" decides the granularity (per-column, per-rule,
    per-permissible-value, or 1 for binary checks). If a check result is
    missing atomic counts, this function raises ``ValueError`` rather than
    silently approximating the score from message counts; populate the
    fields in the check itself (see ``clifpy/utils/validator.py``).
    """
    category_scores = {}
    all_issues: List[Dict[str, Any]] = []

    for category in DQA_CATEGORIES:
        checks = validation_data.get(category, {})
        if not checks:
            continue

        cat_passed = 0
        cat_total = 0

        for check_name, d in checks.items():
            # Enrich this check's messages
            check_enriched: List[Dict[str, Any]] = []
            for err in d['errors']:
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'error',
                    'message': err.get('message', ''),
                    'details': err.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    check_enriched.append(enriched)
            for warn in d['warnings']:
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'warning',
                    'message': warn.get('message', ''),
                    'details': warn.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    check_enriched.append(enriched)
            for info_msg in d.get('info', []):
                issue = {
                    'category': category,
                    'check_type': d['check_type'],
                    'severity': 'info',
                    'message': info_msg.get('message', ''),
                    'details': info_msg.get('details', {}),
                }
                enriched = enrich_issue(issue, check_key=check_name)
                if enriched is not None:
                    check_enriched.append(enriched)

            # Score this check: atomic counts are now mandatory on every
            # DQAResult producer in validator.py. A check reaching this
            # branch without atomic_total/atomic_passed is a bug in the
            # check itself — don't silently absorb it.
            atomic_t = d.get('atomic_total')
            atomic_p = d.get('atomic_passed')
            if atomic_t is None or atomic_p is None:
                raise ValueError(
                    f"Check '{check_name}' in category '{category}' is missing "
                    f"atomic_total/atomic_passed. Every DQA check must populate "
                    f"these fields; see clifpy/utils/validator.py for the pattern."
                )
            cat_total += atomic_t
            cat_passed += atomic_p

            collapsed = _collapse_info_rows(check_enriched)
            _reconcile_atomic_counts(
                collapsed, atomic_t, atomic_p, category, d['check_type'],
                check_key=check_name,
            )
            all_issues.extend(collapsed)

        if cat_total > 0:
            category_scores[category] = (cat_passed, cat_total)

    return category_scores, all_issues


def compute_table_stats(df, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Compute per-column descriptive stats for schema-defined columns.

    Parameters
    ----------
    df : pd.DataFrame or None
        The table's DataFrame.
    schema : dict or None
        The table schema with a ``columns`` list.

    Returns
    -------
    list[dict]
        One dict per column with keys: column, dtype, null_count,
        null_pct, unique.  Empty list when inputs are missing/empty.
    """
    if df is None or schema is None:
        return []
    try:
        n_rows = len(df)
    except Exception:
        return []
    if n_rows == 0:
        return []

    import pandas as pd

    _RANGE_DTYPES = {'DATETIME', 'DATE', 'INT', 'INTEGER', 'DOUBLE', 'FLOAT', 'NUMERIC'}

    schema_cols = [c['name'] for c in schema.get('columns', [])]
    stats: List[Dict[str, Any]] = []
    for col_def in schema.get('columns', []):
        col_name = col_def['name']
        if col_name not in df.columns:
            continue
        series = df[col_name]
        null_count = int(series.isna().sum())
        null_pct = round(null_count / n_rows * 100, 1) if n_rows else 0.0
        unique = int(series.nunique(dropna=True))
        col_dtype = col_def.get('data_type', str(series.dtype)).upper()

        # Compute min/max for numeric and datetime columns
        col_min = None
        col_max = None
        if col_dtype in _RANGE_DTYPES:
            non_null = series.dropna()
            if len(non_null) > 0:
                try:
                    raw_min = non_null.min()
                    raw_max = non_null.max()
                    if col_dtype in ('DATETIME', 'DATE'):
                        fmt = '%Y-%m-%d %H:%M' if col_dtype == 'DATETIME' else '%Y-%m-%d'
                        col_min = pd.Timestamp(raw_min).strftime(fmt)
                        col_max = pd.Timestamp(raw_max).strftime(fmt)
                    else:
                        col_min = str(raw_min)
                        col_max = str(raw_max)
                except Exception:
                    pass

        stats.append({
            'column': col_name,
            'dtype': col_def.get('data_type', str(series.dtype)),
            'null_count': null_count,
            'null_pct': null_pct,
            'unique': unique,
            'min': col_min,
            'max': col_max,
        })
    return stats


def generate_validation_pdf(validation_data: Dict[str, Any],
                            table_name: str, output_path: str,
                            site_name: Optional[str] = None,
                            feedback: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a PDF report from DQA validation results.

    Parameters
    ----------
    validation_data : dict
        Output from run_full_dqa (keys: conformance, completeness,
        relational, plausibility).
    table_name : str
        Name of the table.
    output_path : str
        Path where PDF should be saved.
    site_name : str, optional
        Name of the site/hospital.
    feedback : dict, optional
        User feedback with 'user_decisions' keyed by error_id.

    Returns
    -------
    str
        Path to generated PDF file.
    """
    category_scores, all_issues = collect_dqa_issues(validation_data)

    # Build feedback lookup: error_id -> decision dict
    feedback_lookup: Dict[str, Dict[str, Any]] = {}
    if feedback and feedback.get('user_decisions'):
        feedback_lookup = feedback['user_decisions']

    # Build set of rejected error IDs so we can adjust summary counts
    rejected_ids: set = {
        eid for eid, d in feedback_lookup.items()
        if d.get('decision') == 'rejected'
    }

    total_passed = sum(p for p, _ in category_scores.values())
    total_checks = sum(t for _, t in category_scores.values())
    error_count = sum(i.get('atomic_count', 1) for i in all_issues if i['severity'] == 'error')
    warning_count = sum(i.get('atomic_count', 1) for i in all_issues if i['severity'] == 'warning')

    # Adjust counts: rejected errors no longer count as errors
    if rejected_ids:
        rejected_error_count = sum(
            i.get('atomic_count', 1) for i in all_issues
            if i['severity'] == 'error' and _make_error_id(i) in rejected_ids
        )
        error_count -= rejected_error_count
        total_passed += rejected_error_count

    # --- Build PDF ---
    doc = SimpleDocTemplate(output_path, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    primary_color = colors.HexColor('#1F4E79')
    text_dark = colors.HexColor('#2C3E50')
    text_medium = colors.HexColor('#5D6D7E')
    header_bg = colors.HexColor('#F5F6FA')
    pass_bg = colors.HexColor('#E8F5E8')
    fail_bg = colors.HexColor('#FFEAEA')
    warn_bg = colors.HexColor('#FFF3E0')

    title_style = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'],
        fontSize=22, textColor=primary_color, spaceAfter=24,
        alignment=TA_CENTER, fontName='Helvetica-Bold',
    )
    heading_style = ParagraphStyle(
        'CustomHeading', parent=styles['Heading2'],
        fontSize=14, textColor=text_dark, spaceAfter=10,
        spaceBefore=16, fontName='Helvetica-Bold',
    )
    timestamp_style = ParagraphStyle(
        'TimestampStyle', parent=styles['Normal'],
        fontSize=8, textColor=text_medium, alignment=1,
        fontName='Helvetica',
    )

    # Cell styles for issue detail tables
    cell_style = ParagraphStyle(
        'CellStyle', parent=styles['Normal'],
        fontSize=7, leading=9, fontName='Helvetica',
        textColor=text_dark,
    )
    cell_bold_style = ParagraphStyle(
        'CellBoldStyle', parent=cell_style,
        fontName='Helvetica-Bold',
    )

    # Timestamp
    ts = Table(
        [[Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", timestamp_style)]],
        colWidths=[7.5 * inch],
    )
    ts.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
        ('TOPPADDING', (0, 0), (0, 0), -24),
        ('BOTTOMPADDING', (0, 0), (0, 0), 2),
    ]))
    story.append(ts)

    # Title
    title_text = f"{site_name + ' ' if site_name else ''}CLIF DQA Report Card"
    story.append(Paragraph(title_text, title_style))
    story.append(Paragraph(f"{table_name.title()} Table", heading_style))
    story.append(Spacer(1, 0.2 * inch))

    # --- Feedback banner (only when feedback with decisions exists) ---
    has_feedback = bool(feedback_lookup)
    if has_feedback:
        n_accepted = sum(1 for d in feedback_lookup.values() if d.get('decision') == 'accepted')
        n_rejected = sum(1 for d in feedback_lookup.values() if d.get('decision') == 'rejected')
        if n_accepted > 0 or n_rejected > 0:
            from datetime import datetime as _dt
            _raw_ts = feedback.get('timestamp', '')
            try:
                fb_ts = _dt.fromisoformat(_raw_ts).strftime('%Y-%m-%d %H:%M')
            except (ValueError, TypeError):
                fb_ts = _raw_ts
            banner_style = ParagraphStyle(
                'FeedbackBanner', parent=styles['Normal'],
                fontSize=8, textColor=colors.HexColor('#1F4E79'),
                fontName='Helvetica',
            )
            banner_text = (
                f"<i>This report was updated based on feedback provided on {fb_ts}. "
                f"Accepted: {n_accepted} | Rejected: {n_rejected}</i>"
            )
            banner_tbl = Table(
                [[Paragraph(banner_text, banner_style)]],
                colWidths=[7.5 * inch],
            )
            banner_tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#E8F0FE')),
                ('TOPPADDING', (0, 0), (0, 0), 6),
                ('BOTTOMPADDING', (0, 0), (0, 0), 6),
                ('LEFTPADDING', (0, 0), (0, 0), 10),
                ('RIGHTPADDING', (0, 0), (0, 0), 10),
            ]))
            story.append(banner_tbl)
            story.append(Spacer(1, 0.15 * inch))

    # --- DQA Summary Table ---
    story.append(Paragraph("DQA Summary", heading_style))
    summary_header = ['Category', 'Non-Error', 'Total', 'Errors', 'Warnings']
    summary_rows = [summary_header]
    for category in DQA_CATEGORIES:
        if category not in category_scores:
            continue
        passed, total = category_scores[category]
        cat_issues = [i for i in all_issues if i['category'] == category]
        cat_errors = sum(i.get('atomic_count', 1) for i in cat_issues if i['severity'] == 'error')
        cat_warnings = sum(i.get('atomic_count', 1) for i in cat_issues if i['severity'] == 'warning')
        # Adjust for rejected errors in this category
        if rejected_ids:
            cat_rejected = sum(
                i.get('atomic_count', 1) for i in cat_issues
                if i['severity'] == 'error' and _make_error_id(i) in rejected_ids
            )
            cat_errors -= cat_rejected
            passed += cat_rejected
        summary_rows.append([category.title(), str(passed), str(total),
                             str(cat_errors), str(cat_warnings)])
    summary_rows.append(['Overall', str(total_passed), str(total_checks),
                         str(error_count), str(warning_count)])

    summary_tbl = Table(summary_rows, colWidths=[2 * inch, 0.8 * inch, 0.8 * inch,
                                                  0.8 * inch, 0.8 * inch])
    tbl_style = [
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), header_bg),
        ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
    ]
    # Color-code error/warning cells
    for row_idx in range(1, len(summary_rows)):
        errors_val = int(summary_rows[row_idx][3])
        warnings_val = int(summary_rows[row_idx][4])
        if errors_val > 0:
            tbl_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), fail_bg))
        else:
            tbl_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), pass_bg))
        if warnings_val > 0:
            tbl_style.append(('BACKGROUND', (4, row_idx), (4, row_idx), warn_bg))
        else:
            tbl_style.append(('BACKGROUND', (4, row_idx), (4, row_idx), pass_bg))
    summary_tbl.setStyle(TableStyle(tbl_style))
    story.append(summary_tbl)
    story.append(Spacer(1, 0.3 * inch))

    # --- Data Profile Table ---
    table_stats = validation_data.get('table_stats', [])
    if table_stats:
        story.append(Paragraph("Data Profile", heading_style))
        total_rows = validation_data.get('total_rows', 0)
        profile_subtitle = ParagraphStyle(
            'ProfileSubtitle', parent=styles['Normal'],
            fontSize=9, textColor=text_medium, fontName='Helvetica',
        )
        story.append(Paragraph(f"Total Rows: {total_rows:,}", profile_subtitle))
        story.append(Spacer(1, 0.1 * inch))

        profile_header = ['Column', 'Dtype', 'Null', 'Null%', 'Unique', 'Min', 'Max']
        profile_rows = [profile_header]
        for s in table_stats:
            profile_rows.append([
                s['column'], s['dtype'],
                f"{s['null_count']:,}",
                f"{s['null_pct']:.1f}%", f"{s['unique']:,}",
                s.get('min') or '', s.get('max') or '',
            ])

        profile_col_widths = [1.6 * inch, 0.9 * inch,
                              0.6 * inch, 0.6 * inch, 0.6 * inch,
                              1.3 * inch, 1.3 * inch]
        profile_tbl = Table(profile_rows, colWidths=profile_col_widths)

        profile_style = [
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('BACKGROUND', (0, 0), (-1, 0), header_bg),
            ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            # Right-align numeric columns (Non-Null, Null, Null%, Unique)
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ]
        # Color-code Null% cells
        amber_bg = colors.HexColor('#FFF3E0')
        red_light_bg = colors.HexColor('#FFEAEA')
        for row_idx, s in enumerate(table_stats, 1):
            if s['null_pct'] > 50:
                profile_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), red_light_bg))
            elif s['null_pct'] > 10:
                profile_style.append(('BACKGROUND', (3, row_idx), (3, row_idx), amber_bg))

        profile_tbl.setStyle(TableStyle(profile_style))
        story.append(profile_tbl)
        story.append(Spacer(1, 0.3 * inch))

    # --- Per-category issue details as structured tables ---
    rejected_bg = colors.HexColor('#E0E0E0')
    rejected_text = colors.HexColor('#999999')

    if all_issues:
        story.append(PageBreak())
        story.append(Paragraph("Details", heading_style))

        # Column order: rule, rule_description, column_field, severity, finding,
        # checks, [status]. Metric is omitted because each sub-table is already
        # a single category.
        if has_feedback:
            detail_col_widths = [0.5 * inch, 1.7 * inch, 1.1 * inch,
                                 0.5 * inch, 2.7 * inch, 0.5 * inch, 0.5 * inch]
        else:
            detail_col_widths = [0.5 * inch, 1.7 * inch, 1.1 * inch,
                                 0.5 * inch, 3.2 * inch, 0.5 * inch]
        checks_col_idx = 5

        for category in DQA_CATEGORIES:
            cat_issues = [i for i in all_issues if i['category'] == category]
            if not cat_issues:
                continue

            cs = category_scores.get(category)
            header_tail = f"({cs[0]}/{cs[1]})" if cs else f"({len(cat_issues)})"
            story.append(Paragraph(f"{category.title()} {header_tail}", heading_style))

            # Header row
            header_row = [
                Paragraph('<b>rule</b>', cell_bold_style),
                Paragraph('<b>rule_description</b>', cell_bold_style),
                Paragraph('<b>column_field</b>', cell_bold_style),
                Paragraph('<b>severity</b>', cell_bold_style),
                Paragraph('<b>finding</b>', cell_bold_style),
                Paragraph('<b>checks</b>', cell_bold_style),
            ]
            if has_feedback:
                header_row.append(Paragraph('<b>status</b>', cell_bold_style))
            table_data = [header_row]

            for issue in cat_issues:
                severity_upper = issue['severity'].upper()
                finding_text = truncate_comment(issue.get('finding', issue['message']))
                # Build finding cell: text + optional sparkline for temporal checks
                spark_width = 2.5 * inch if has_feedback else 3.0 * inch
                yearly_counts = issue.get('details', {}).get('yearly_counts')
                if yearly_counts:
                    finding_cell = [
                        Paragraph(escape(finding_text), cell_style),
                        Spacer(1, 2),
                        YearlySparkBar(yearly_counts, width=spark_width, height=16),
                    ]
                else:
                    finding_cell = Paragraph(escape(finding_text), cell_style)
                row = [
                    Paragraph(escape(issue.get('rule_code', '')), cell_style),
                    Paragraph(escape(issue.get('rule_description', '')), cell_style),
                    Paragraph(escape(issue.get('column_field', 'NA')), cell_style),
                    Paragraph(escape(severity_upper), cell_style),
                    finding_cell,
                    Paragraph(
                        '—' if issue.get('atomic_count', 1) == 0
                        else str(issue.get('atomic_count', 1)),
                        cell_style,
                    ),
                ]
                if has_feedback:
                    if issue['severity'] == 'error':
                        error_id = _make_error_id(issue)
                        decision_info = feedback_lookup.get(error_id, {})
                        status_text = decision_info.get('decision', '').upper()
                        row.append(Paragraph(escape(status_text), cell_style))
                    else:
                        row.append(Paragraph('', cell_style))
                table_data.append(row)

            detail_tbl = Table(table_data, colWidths=detail_col_widths,
                               repeatRows=1)

            # Base table style
            detail_style = [
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 7),
                ('BACKGROUND', (0, 0), (-1, 0), header_bg),
                ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (checks_col_idx, 0), (checks_col_idx, -1), 'RIGHT'),
            ]

            # Color-code rows by severity, override with grey for rejected
            for row_idx, issue in enumerate(cat_issues, 1):
                error_id = _make_error_id(issue)
                decision = feedback_lookup.get(error_id, {}).get('decision', '')
                if decision == 'rejected':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), rejected_bg))
                    detail_style.append(('TEXTCOLOR', (0, row_idx), (-1, row_idx), rejected_text))
                elif issue['severity'] == 'error':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), fail_bg))
                elif issue['severity'] == 'warning':
                    detail_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), warn_bg))

            detail_tbl.setStyle(TableStyle(detail_style))
            story.append(detail_tbl)

            # Add note about monthly trend CSVs for temporal consistency checks
            temporal_cols = sorted({
                i['column_field'] for i in cat_issues
                if i.get('check_type') == 'category_temporal_consistency'
                and i.get('column_field')
            })
            if temporal_cols:
                file_list = ", ".join(
                    f"{table_name}_{col}_monthly.csv" for col in temporal_cols
                )
                note_text = (
                    f"<i>P.6 Category temporal consistency — monthly breakdown available at: "
                    f"output/final/validation/monthly_trends/ ({file_list})</i>"
                )
                note_style = ParagraphStyle(
                    'NoteStyle', parent=styles['Normal'],
                    fontSize=7, textColor=colors.HexColor('#555555'),
                )
                story.append(Paragraph(note_text, note_style))

            story.append(Spacer(1, 0.2 * inch))
    else:
        story.append(Paragraph("No validation issues found!", styles['Normal']))

    doc.build(story)
    return output_path


def generate_text_report(validation_data: Dict[str, Any],
                         table_name: str, output_path: str,
                         site_name: Optional[str] = None) -> str:
    """
    Generate a plain-text DQA report.

    Parameters match generate_validation_pdf.
    """
    category_scores, all_issues = collect_dqa_issues(validation_data)
    total_passed = sum(p for p, _ in category_scores.values())
    total_checks = sum(t for _, t in category_scores.values())
    error_count = sum(i.get('atomic_count', 1) for i in all_issues if i['severity'] == 'error')
    warning_count = sum(i.get('atomic_count', 1) for i in all_issues if i['severity'] == 'warning')

    lines = []
    lines.append("=" * 120)
    lines.append("CLIF 2.1 DQA VALIDATION REPORT")
    lines.append(f"{table_name.upper()} TABLE")
    lines.append("=" * 120)
    lines.append("")
    if site_name:
        lines.append(f"Site: {site_name}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # DQA Summary
    lines.append("-" * 120)
    lines.append("DQA SUMMARY")
    lines.append("-" * 120)
    lines.append(f"  {'Category':20s}  {'Passed':>6s}  {'Total':>5s}  {'Errors':>6s}  {'Warnings':>8s}")
    lines.append(f"  {'-'*20}  {'-'*6}  {'-'*5}  {'-'*6}  {'-'*8}")
    for category in DQA_CATEGORIES:
        if category not in category_scores:
            continue
        passed, total = category_scores[category]
        cat_issues = [i for i in all_issues if i['category'] == category]
        cat_errors = sum(i.get('atomic_count', 1) for i in cat_issues if i['severity'] == 'error')
        cat_warnings = sum(i.get('atomic_count', 1) for i in cat_issues if i['severity'] == 'warning')
        lines.append(f"  {category.title():20s}  {passed:>6d}  {total:>5d}  {cat_errors:>6d}  {cat_warnings:>8d}")
    lines.append(f"  {'Overall':20s}  {total_passed:>6d}  {total_checks:>5d}  {error_count:>6d}  {warning_count:>8d}")
    lines.append("")

    # Data Profile
    table_stats = validation_data.get('table_stats', [])
    if table_stats:
        lines.append("-" * 120)
        lines.append("DATA PROFILE")
        lines.append("-" * 120)
        total_rows = validation_data.get('total_rows', 0)
        lines.append(f"  Total Rows: {total_rows:,}")
        lines.append("")
        w_col, w_dtype, w_null, w_pct, w_uniq, w_min, w_max = 25, 12, 8, 8, 10, 20, 20
        hdr = (f"  {'Column':<{w_col}}"
               f"{'Dtype':<{w_dtype}}"
               f"{'Null':>{w_null}}"
               f"{'Null%':>{w_pct}}"
               f"{'Unique':>{w_uniq}}"
               f"  {'Min':<{w_min}}"
               f"{'Max':<{w_max}}")
        lines.append(hdr)
        lines.append("  " + "-" * (w_col + w_dtype + w_null + w_pct + w_uniq + 2 + w_min + w_max))
        for s in table_stats:
            col_name = s['column']
            if len(col_name) > w_col - 2:
                col_name = col_name[:w_col - 4] + '..'
            col_min = s.get('min') or ''
            col_max = s.get('max') or ''
            lines.append(
                f"  {col_name:<{w_col}}"
                f"{s['dtype']:<{w_dtype}}"
                f"{s['null_count']:>{w_null},}"
                f"{s['null_pct']:>{w_pct}.1f}%"
                f"{s['unique']:>{w_uniq},}"
                f"  {col_min:<{w_min}}"
                f"{col_max:<{w_max}}"
            )
        lines.append("")

    # Issue details as tabular text
    if all_issues:
        lines.append("=" * 120)
        lines.append("DETAILS")
        lines.append("=" * 120)

        # Column widths for text alignment
        w_rule, w_desc, w_col, w_sev, w_checks = 6, 30, 18, 10, 7

        for category in DQA_CATEGORIES:
            cat_issues = [i for i in all_issues if i['category'] == category]
            if not cat_issues:
                continue

            lines.append("")
            cs = category_scores.get(category)
            header_tail = f"({cs[0]}/{cs[1]})" if cs else f"({len(cat_issues)})"
            lines.append(f"-- {category.title()} {header_tail} --")
            lines.append("")

            # Header
            hdr = (f"  {'rule':<{w_rule}}"
                   f"{'rule_description':<{w_desc}}"
                   f"{'column_field':<{w_col}}"
                   f"{'severity':<{w_sev}}"
                   f"{'checks':>{w_checks}}  "
                   f"finding")
            lines.append(hdr)
            lines.append("  " + "-" * 116)

            for issue in cat_issues:
                severity_upper = issue['severity'].upper()
                rule_code = issue.get('rule_code', '')
                rule_desc = issue.get('rule_description', '')
                col_field = issue.get('column_field', 'NA')
                finding = issue.get('finding', issue['message'])
                checks = issue.get('atomic_count', 1)
                checks_str = '—' if checks == 0 else f"{checks:d}"

                # Truncate long fields for text display
                if len(rule_desc) > w_desc - 2:
                    rule_desc = rule_desc[:w_desc - 4] + '..'
                if len(col_field) > w_col - 2:
                    col_field = col_field[:w_col - 4] + '..'

                line = (f"  {rule_code:<{w_rule}}"
                        f"{rule_desc:<{w_desc}}"
                        f"{col_field:<{w_col}}"
                        f"{severity_upper:<{w_sev}}"
                        f"{checks_str:>{w_checks}s}  "
                        f"{finding}")
                lines.append(line)
    else:
        lines.append("No validation issues found!")

    lines.append("")
    lines.append("=" * 120)
    lines.append("END OF REPORT")
    lines.append("=" * 120)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return output_path


# ---------------------------------------------------------------------------
# Combined (multi-table) report generation
# ---------------------------------------------------------------------------

TABLE_DISPLAY_NAMES: Dict[str, str] = {
    'adt': 'ADT',
    'code_status': 'Code Status',
    'crrt_therapy': 'CRRT Therapy',
    'ecmo_mcs': 'ECMO/MCS',
    'hospital_diagnosis': 'Hospital Diagnosis',
    'hospitalization': 'Hospitalization',
    'labs': 'Labs',
    'medication_admin_continuous': 'Medication Admin Continuous',
    'medication_admin_intermittent': 'Medication Admin Intermittent',
    'microbiology_culture': 'Microbiology Culture',
    'microbiology_nonculture': 'Microbiology Nonculture',
    'microbiology_susceptibility': 'Microbiology Susceptibility',
    'patient': 'Patient',
    'patient_assessments': 'Patient Assessments',
    'patient_procedures': 'Patient Procedures',
    'position': 'Position',
    'respiratory_support': 'Respiratory Support',
    'vitals': 'Vitals',
}


def collect_table_results(
    json_dir: str,
    table_names: List[str],
    feedback_dir: Optional[str] = None,
):
    """Load per-table DQA JSON results and optional feedback files.

    Parameters
    ----------
    json_dir : str
        Directory containing ``{table_name}_dqa.json`` files.
    table_names : list[str]
        Ordered list of table names to include.
    feedback_dir : str, optional
        Directory containing ``{table_name}_validation_response.json``
        feedback files.  When *None*, no feedback is loaded.

    Returns
    -------
    (dict, dict)
        ``(results, feedback_map)`` where both map
        ``table_name -> dict | None``.
    """
    results: Dict[str, Any] = {}
    feedback_map: Dict[str, Any] = {}

    for table_name in table_names:
        json_path = os.path.join(json_dir, f'{table_name}_dqa.json')
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    results[table_name] = json.load(f)
            except Exception:
                results[table_name] = None
        else:
            results[table_name] = None

        if feedback_dir:
            fb_path = os.path.join(feedback_dir, f'{table_name}_validation_response.json')
            if os.path.exists(fb_path):
                try:
                    with open(fb_path, 'r', encoding='utf-8') as f:
                        feedback_map[table_name] = json.load(f)
                except Exception:
                    feedback_map[table_name] = None
            else:
                feedback_map[table_name] = None

    return results, feedback_map



def _clif_version_from_results(table_results: Dict[str, Any]) -> Optional[str]:
    """Recover the CLIF version a set of DQA results was produced at.

    Absent tables have no schema of their own to read a version from, but their
    ``0/N`` denominators have to be computed against the same version as the
    tables that were submitted — otherwise the two are not comparable. Every
    present result carries the version ``run_full_dqa`` stamped on it, so take
    it from there rather than making callers pass it twice.

    Returns None when nothing in *table_results* reports a version, in which case
    the caller falls back to the package default.
    """
    for result in (table_results or {}).values():
        if isinstance(result, dict):
            version = result.get('clif_version')
            if version:
                return str(version)
    return None

def generate_combined_validation_pdf(
    table_results: Dict[str, Any],
    output_path: str,
    table_names: List[str],
    site_name: Optional[str] = None,
    feedback_map: Optional[Dict[str, Any]] = None,
    display_names: Optional[Dict[str, str]] = None,
) -> str:
    """Generate a combined PDF report from multiple table DQA results.

    Creates the "DQA Overview" table with one row per CLIF table and
    columns for Conformance, Completeness, Plausibility, and Overall.

    For tables with no validation data, the conformance denominator is
    computed from the schema via
    :func:`~clifpy.utils.validator.get_schema_check_counts` so the
    report shows ``0/N`` instead of ``N/A``.

    Parameters
    ----------
    table_results : dict
        Mapping of ``table_name -> serialized DQA result dict`` (or
        *None* for tables that were not validated).
    output_path : str
        Path for the output PDF file.
    table_names : list[str]
        Ordered list of table names (controls row order).
    site_name : str, optional
        Site/hospital label for the report title.
    feedback_map : dict, optional
        Mapping of ``table_name -> feedback dict`` with user decisions.
    display_names : dict, optional
        Custom ``table_name -> display label`` mapping.  Falls back to
        :data:`TABLE_DISPLAY_NAMES`.

    Returns
    -------
    str
        Path to the generated PDF.
    """
    from clifpy.utils.validator import build_absent_table_dqa_result
    from clifpy.schemas import DEFAULT_CLIF_VERSION

    resolved_version = _clif_version_from_results(table_results) or DEFAULT_CLIF_VERSION

    if feedback_map is None:
        feedback_map = {}
    if display_names is None:
        display_names = TABLE_DISPLAY_NAMES

    doc = SimpleDocTemplate(output_path, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    primary_color = colors.HexColor('#1F4E79')
    text_dark = colors.HexColor('#2C3E50')
    text_medium = colors.HexColor('#5D6D7E')
    header_bg = colors.HexColor('#F5F6FA')
    pass_bg = colors.HexColor('#E8F5E8')
    fail_bg = colors.HexColor('#FFEAEA')

    title_style = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'],
        fontSize=22, textColor=primary_color, spaceAfter=24,
        alignment=TA_CENTER, fontName='Helvetica-Bold',
    )
    heading_style = ParagraphStyle(
        'CustomHeading', parent=styles['Heading2'],
        fontSize=14, textColor=text_dark, spaceAfter=10,
        spaceBefore=16, fontName='Helvetica-Bold',
    )
    normal_style = ParagraphStyle(
        'CustomNormal', parent=styles['Normal'],
        fontSize=9, textColor=text_medium, fontName='Helvetica',
    )
    timestamp_style = ParagraphStyle(
        'TimestampStyle', parent=styles['Normal'],
        fontSize=8, textColor=text_medium, alignment=1,
        fontName='Helvetica',
    )

    # Timestamp
    ts = Table(
        [[Paragraph(
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            timestamp_style,
        )]],
        colWidths=[7.5 * inch],
    )
    ts.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
        ('TOPPADDING', (0, 0), (0, 0), -24),
        ('BOTTOMPADDING', (0, 0), (0, 0), 2),
    ]))
    story.append(ts)

    # Title
    title_text = f"{site_name + ' ' if site_name else ''}CLIF DQA Report Card"
    story.append(Paragraph(title_text, title_style))
    story.append(Paragraph("Combined Validation Report", heading_style))
    story.append(Spacer(1, 0.2 * inch))

    # Check if any table has feedback
    has_any_feedback = any(
        fb and any(d.get('decision') in ('accepted', 'rejected')
                   for d in fb.get('user_decisions', {}).values())
        for fb in feedback_map.values() if fb
    )

    # Feedback banner
    if has_any_feedback:
        fb_banner_style = ParagraphStyle(
            'FbBanner', parent=normal_style, fontSize=8,
            textColor=colors.HexColor('#1F4E79'), fontName='Helvetica',
        )
        fb_banner = Table(
            [[Paragraph(
                "<i>This report includes user feedback decisions. "
                "See individual table reports for details.</i>",
                fb_banner_style,
            )]],
            colWidths=[7.5 * inch],
        )
        fb_banner.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#E8F0FE')),
            ('TOPPADDING', (0, 0), (0, 0), 6),
            ('BOTTOMPADDING', (0, 0), (0, 0), 6),
            ('LEFTPADDING', (0, 0), (0, 0), 10),
            ('RIGHTPADDING', (0, 0), (0, 0), 10),
        ]))
        story.append(fb_banner)
        story.append(Spacer(1, 0.15 * inch))

    # --- DQA Overview table ---
    story.append(Paragraph("DQA Overview", heading_style))
    cat_labels = [c.title() for c in DQA_CATEGORIES]
    overview_header = ['Table'] + cat_labels + ['Overall']
    if has_any_feedback:
        overview_header.append('Feedback')
    overview_rows = [overview_header]

    n_score_cols = len(DQA_CATEGORIES) + 1  # categories + Overall

    def _row_overall(row):
        """Sum p/t across category columns in a row."""
        total_p, total_t = 0, 0
        for cell in row[1:1 + len(DQA_CATEGORIES)]:
            if cell == 'N/A':
                continue
            parts = cell.split('/')
            if len(parts) == 2:
                total_p += int(parts[0])
                total_t += int(parts[1])
        return f"{total_p}/{total_t}" if total_t > 0 else 'N/A'

    def _feedback_summary(fb):
        """Compact feedback summary like '2R/1A'."""
        if not fb or not fb.get('user_decisions'):
            return ''
        n_a = sum(1 for d in fb['user_decisions'].values()
                  if d.get('decision') == 'accepted')
        n_r = sum(1 for d in fb['user_decisions'].values()
                  if d.get('decision') == 'rejected')
        parts = []
        if n_r:
            parts.append(f"{n_r}R")
        if n_a:
            parts.append(f"{n_a}A")
        return '/'.join(parts)

    for table_name in table_names:
        dqa_data = table_results.get(table_name)
        label = display_names.get(
            table_name, table_name.replace('_', ' ').title())

        if dqa_data is None or dqa_data.get('absent'):
            # Table was not submitted. Use the canonical absent-result
            # helper so denominators and messaging come from one place.
            if dqa_data is None:
                dqa_data = build_absent_table_dqa_result(table_name, clif_version=resolved_version)
            expected = dqa_data.get(
                'expected_check_counts',
                build_absent_table_dqa_result(
                    table_name, clif_version=resolved_version
                )['expected_check_counts'],
            )
            row = [label]
            for cat in DQA_CATEGORIES:
                # Absent tables only contribute conformance atoms — those
                # checks (table_presence, required columns, dtypes …) are
                # schema-derivable and legitimately fail when the table is
                # missing. Completeness and plausibility need actual data
                # to evaluate, so they render as N/A and do not inflate
                # the denominators in the totals row below.
                if cat == 'conformance':
                    n = expected.get(cat, 0)
                    row.append(f"0/{n}" if n > 0 else 'N/A')
                else:
                    row.append('N/A')
            row.append(_row_overall(row))
            if has_any_feedback:
                row.append('')
            overview_rows.append(row)
            continue

        scores, all_issues = collect_dqa_issues(dqa_data)

        # Adjust scores for rejected feedback
        fb = feedback_map.get(table_name)
        rejected_ids: set = set()
        if fb and fb.get('user_decisions'):
            rejected_ids = {
                eid for eid, d in fb['user_decisions'].items()
                if d.get('decision') == 'rejected'
            }
        if rejected_ids:
            for cat in list(scores.keys()):
                cat_rejected = sum(
                    i.get('atomic_count', 1) for i in all_issues
                    if i['category'] == cat and i['severity'] == 'error'
                    and _make_error_id(i) in rejected_ids
                )
                if cat_rejected:
                    p, t = scores[cat]
                    scores[cat] = (p + cat_rejected, t)

        row = [label]
        for cat in DQA_CATEGORIES:
            if cat in scores:
                p, t = scores[cat]
                row.append(f"{p}/{t}")
            else:
                row.append('N/A')
        row.append(_row_overall(row))
        if has_any_feedback:
            row.append(_feedback_summary(feedback_map.get(table_name)))
        overview_rows.append(row)

    # Totals row
    totals_row = ['Total']
    for col_idx in range(1, len(DQA_CATEGORIES) + 1):
        total_passed, total_count = 0, 0
        for row in overview_rows[1:]:
            cell_val = row[col_idx]
            if cell_val == 'N/A':
                continue
            parts = cell_val.split('/')
            if len(parts) == 2:
                total_passed += int(parts[0])
                total_count += int(parts[1])
        totals_row.append(
            f"{total_passed}/{total_count}" if total_count > 0 else 'N/A')
    totals_row.append(_row_overall(totals_row))
    if has_any_feedback:
        totals_row.append('')
    overview_rows.append(totals_row)

    # Build table
    col_widths = [2.2 * inch] + [1.2 * inch] * (len(DQA_CATEGORIES) + 1)
    if has_any_feedback:
        col_widths.append(0.7 * inch)
    overview_tbl = Table(overview_rows, colWidths=col_widths)

    totals_row_idx = len(overview_rows) - 1
    tbl_style = [
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, totals_row_idx), (-1, totals_row_idx),
         'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), header_bg),
        ('BACKGROUND', (0, totals_row_idx), (-1, totals_row_idx), header_bg),
        ('TEXTCOLOR', (0, 0), (-1, -1), text_dark),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#DADADA')),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
    ]
    # Color-code pass/fail cells
    for row_idx in range(1, len(overview_rows)):
        for col_idx in range(1, n_score_cols + 1):
            cell_val = overview_rows[row_idx][col_idx]
            if cell_val == 'N/A':
                continue
            parts = cell_val.split('/')
            if len(parts) == 2 and parts[0] == parts[1]:
                tbl_style.append(
                    ('BACKGROUND', (col_idx, row_idx),
                     (col_idx, row_idx), pass_bg))
            else:
                tbl_style.append(
                    ('BACKGROUND', (col_idx, row_idx),
                     (col_idx, row_idx), fail_bg))

    overview_tbl.setStyle(TableStyle(tbl_style))
    story.append(overview_tbl)
    story.append(Spacer(1, 0.3 * inch))

    doc.build(story)
    return output_path


def generate_consolidated_csv(
    table_results: Dict[str, Any],
    output_path: str,
    table_names: List[str],
    feedback_map: Optional[Dict[str, Any]] = None,
    display_names: Optional[Dict[str, str]] = None,
) -> str:
    """Generate a consolidated CSV from multiple table DQA results.

    Produces one row per issue across all tables, with feedback decisions
    where available.

    Parameters
    ----------
    table_results : dict
        Mapping of ``table_name -> serialized DQA result dict`` (or *None*).
    output_path : str
        Path for the output CSV file.
    table_names : list[str]
        Ordered list of table names.
    feedback_map : dict, optional
        Mapping of ``table_name -> feedback dict``.
    display_names : dict, optional
        Custom ``table_name -> display label`` mapping.

    Returns
    -------
    str
        Path to the generated CSV.
    """
    from clifpy.utils.validator import build_absent_table_dqa_result
    from clifpy.schemas import DEFAULT_CLIF_VERSION

    resolved_version = _clif_version_from_results(table_results) or DEFAULT_CLIF_VERSION

    if feedback_map is None:
        feedback_map = {}
    if display_names is None:
        display_names = TABLE_DISPLAY_NAMES

    rows = []
    for table_name in table_names:
        dqa_data = table_results.get(table_name)
        label = display_names.get(
            table_name, table_name.replace('_', ' ').title())
        fb = feedback_map.get(table_name)
        fb_decisions = fb.get('user_decisions', {}) if fb else {}

        if dqa_data is None or dqa_data.get('absent'):
            if dqa_data is None:
                dqa_data = build_absent_table_dqa_result(table_name, clif_version=resolved_version)
            presence = dqa_data.get('conformance', {}).get('table_presence', {})
            msg = (
                presence.get('errors', [{}])[0].get('message')
                or 'Table not present in dataset'
            )
            rows.append({
                'table_name': label,
                'category': 'conformance',
                'rule_code': 'C.1',
                'rule_description': 'table_presence',
                'check_type': 'Table Status',
                'column_field': 'NA',
                'severity': 'error',
                'passed': False,
                'message': msg,
                'checks': 1,
                'decision': '',
                'reason': '',
            })
            continue

        _, all_issues = collect_dqa_issues(dqa_data)

        if not all_issues:
            rows.append({
                'table_name': label,
                'category': '',
                'rule_code': '',
                'rule_description': '',
                'check_type': 'Summary',
                'column_field': 'NA',
                'severity': 'info',
                'passed': True,
                'message': 'All DQA checks passed',
                'checks': 0,
                'decision': '',
                'reason': '',
            })
            continue

        for issue in all_issues:
            error_id = _make_error_id(issue)
            decision_info = fb_decisions.get(error_id, {})
            rows.append({
                'table_name': label,
                'category': issue['category'],
                'rule_code': issue.get('rule_code', ''),
                'rule_description': issue.get('rule_description', ''),
                'check_type': issue['check_type'],
                'column_field': issue.get('column_field', 'NA'),
                'severity': issue['severity'],
                'passed': False,
                'message': issue.get('finding', issue['message']),
                'checks': issue.get('atomic_count', 1),
                'decision': (decision_info.get('decision', '')
                             if issue['severity'] == 'error' else ''),
                'reason': (decision_info.get('reason', '')
                           if issue['severity'] == 'error' else ''),
            })

    fieldnames = [
        'table_name', 'category', 'rule_code', 'rule_description',
        'check_type', 'column_field', 'severity', 'passed', 'message',
        'checks', 'decision', 'reason',
    ]
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def generate_combined_report(
    json_dir: str,
    output_dir: str,
    table_names: List[str],
    site_name: Optional[str] = None,
    feedback_dir: Optional[str] = None,
) -> Optional[str]:
    """Generate a combined DQA report (PDF + CSV) from JSON result files.

    High-level convenience function that loads per-table JSON results
    and produces both a combined PDF overview and a consolidated CSV.

    Parameters
    ----------
    json_dir : str
        Directory containing ``{table_name}_dqa.json`` files.
    output_dir : str
        Directory where the PDF and CSV will be written.
    table_names : list[str]
        Ordered list of table names to include.
    site_name : str, optional
        Site/hospital label for the report title.
    feedback_dir : str, optional
        Directory containing ``{table_name}_validation_response.json``
        files.

    Returns
    -------
    str or None
        Path to generated PDF, or *None* on failure.
    """
    try:
        table_results, feedback_map = collect_table_results(
            json_dir, table_names, feedback_dir)

        analyzed_count = sum(1 for r in table_results.values()
                            if r is not None)
        if analyzed_count == 0:
            return None

        os.makedirs(output_dir, exist_ok=True)
        pdf_path = os.path.join(output_dir, 'combined_validation_report.pdf')
        generate_combined_validation_pdf(
            table_results, pdf_path, table_names, site_name,
            feedback_map=feedback_map)

        csv_path = os.path.join(output_dir, 'consolidated_validation.csv')
        generate_consolidated_csv(
            table_results, csv_path, table_names,
            feedback_map=feedback_map)

        return pdf_path

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None
