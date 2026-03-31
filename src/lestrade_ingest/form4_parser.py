from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Literal

from lxml import etree


@dataclass
class ParsedTransaction:
    transaction_index: int
    transaction_category: Literal["non_derivative", "derivative"]
    security_title: str | None = None
    transaction_date: date | None = None
    transaction_code: str | None = None
    acquired_disposed_code: str | None = None
    shares: Decimal | None = None
    price_per_share: Decimal | None = None
    total_value: Decimal | None = None
    direct_indirect: str | None = None
    footnote_ids: list[str] = field(default_factory=list)


@dataclass
class ParsedFiling:
    document_type: str
    is_amendment: bool
    schema_version: str | None
    period_of_report: date | None
    issuer_cik: str
    issuer_name: str | None
    issuer_ticker: str | None
    insider_cik: str | None
    insider_name: str | None
    insider_title: str | None
    transactions: list[ParsedTransaction] = field(default_factory=list)
    parse_warnings: list[str] = field(default_factory=list)


def _first_local(root: etree._Element, name: str) -> etree._Element | None:
    if etree.QName(root).localname == name:
        return root
    found = root.xpath(f".//*[local-name()='{name}']")
    return found[0] if found else None


def _text_local(parent: etree._Element | None, name: str) -> str | None:
    if parent is None:
        return None
    el = _first_local(parent, name)
    if el is None or el.text is None:
        return None
    t = el.text.strip()
    return t if t else None


def _decimal_local(parent: etree._Element | None, path_names: tuple[str, ...]) -> Decimal | None:
    """Follow child tags by local name; return Decimal from last node's text or value child."""
    cur: etree._Element | None = parent
    for n in path_names:
        if cur is None:
            return None
        cur = _first_local(cur, n)
    if cur is None:
        return None
    v_el = _first_local(cur, "value")
    raw = (v_el.text if v_el is not None and v_el.text else None) or (
        cur.text.strip() if cur.text else None
    )
    if raw is None or raw == "":
        return None
    if any(c.isalpha() for c in raw):
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _footnote_ids_on(el: etree._Element | None) -> list[str]:
    if el is None:
        return []
    out: list[str] = []
    for attr in ("footnoteId", "footnoteID", "id"):
        v = el.get(attr)
        if v:
            out.append(v)
    return out


def _collect_amount_footnotes(tx: etree._Element) -> list[str]:
    ids: list[str] = []
    for tag in ("transactionShares", "transactionPricePerShare", "transactionAcquiredDisposedCode"):
        node = _first_local(tx, tag)
        if node is None:
            continue
        ids.extend(_footnote_ids_on(node))
        v = _first_local(node, "value")
        if v is not None:
            ids.extend(_footnote_ids_on(v))
    return list(dict.fromkeys(ids))


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _security_title_text(tx: etree._Element) -> str | None:
    st = _first_local(tx, "securityTitle")
    if st is None:
        return None
    v = _text_local(st, "value")
    if v:
        return v
    t = "".join(st.itertext()).strip()
    return t or None


def _pad_cik(s: str | None) -> str | None:
    if not s:
        return None
    d = "".join(c for c in s if c.isdigit())
    if not d:
        return None
    return d.zfill(10)[:10]


def _flatten_insider_title(rel: etree._Element | None) -> str | None:
    if rel is None:
        return None
    parts: list[str] = []
    if _text_local(rel, "isOfficer") in ("1", "true", "True"):
        ot = _text_local(rel, "officerTitle")
        if ot:
            parts.append(ot)
    if _text_local(rel, "isDirector") in ("1", "true", "True"):
        parts.append("director")
    if _text_local(rel, "isTenPercentOwner") in ("1", "true", "True"):
        parts.append("ten percent owner")
    if _text_local(rel, "isOther") in ("1", "true", "True"):
        o = _text_local(rel, "otherText")
        if o:
            parts.append(o)
    if not parts:
        return None
    return "; ".join(parts)


def _derivative_total_value() -> Decimal | None:
    return None


def parse_form4_xml(xml_bytes: bytes) -> ParsedFiling:
    root = etree.fromstring(xml_bytes)
    od = _first_local(root, "ownershipDocument")
    if od is None:
        raise ValueError("missing ownershipDocument root")

    doc_type = _text_local(od, "documentType")
    if doc_type is None:
        raise ValueError("missing documentType")
    doc_type_norm = doc_type.strip()
    if doc_type_norm not in ("4", "4/A"):
        raise ValueError(f"unsupported documentType {doc_type_norm!r}")

    issuer = _first_local(od, "issuer")
    issuer_cik_raw = _text_local(issuer, "issuerCik") if issuer is not None else None
    issuer_cik = _pad_cik(issuer_cik_raw)
    if not issuer_cik:
        raise ValueError("missing issuerCik")

    insider_cik = None
    insider_name = None
    insider_title = None
    ro = _first_local(od, "reportingOwner")
    if ro is not None:
        rid = _first_local(ro, "reportingOwnerId")
        insider_cik = _pad_cik(_text_local(rid, "rptOwnerCik") if rid is not None else None)
        insider_name = _text_local(rid, "rptOwnerName") if rid is not None else None
        rel = _first_local(ro, "reportingOwnerRelationship")
        insider_title = _flatten_insider_title(rel)

    warnings: list[str] = []
    owners = od.xpath(".//*[local-name()='reportingOwner']")
    if len(owners) > 1:
        warnings.append(f"multiple reportingOwner blocks ({len(owners)}); using first only")

    schema_version = _text_local(od, "schemaVersion")
    period = _parse_date(_text_local(od, "periodOfReport"))

    txs: list[ParsedTransaction] = []
    idx = 0

    nd_table = _first_local(od, "nonDerivativeTable")
    if nd_table is not None:
        for tx in nd_table.xpath(".//*[local-name()='nonDerivativeTransaction']"):
            security_title = _security_title_text(tx)

            tdate = _parse_date(_text_local(tx, "transactionDate"))
            coding = _first_local(tx, "transactionCoding")
            tcode = _text_local(coding, "transactionCode") if coding is not None else None
            _ta = _first_local(tx, "transactionAmounts")
            ad = _first_local(_ta if _ta is not None else tx, "transactionAcquiredDisposedCode")
            ad_code = None
            if ad is not None:
                v = _first_local(ad, "value")
                raw = (v.text or "").strip() if v is not None and v.text else (ad.text or "").strip()
                if raw in ("A", "D"):
                    ad_code = raw
                elif raw:
                    ad_code = raw[:1].upper()

            shares = _decimal_local(tx, ("transactionAmounts", "transactionShares"))
            price = _decimal_local(tx, ("transactionAmounts", "transactionPricePerShare"))
            fn_ids = _collect_amount_footnotes(tx)

            own = _first_local(tx, "ownershipNature")
            dio = _text_local(own, "directOrIndirectOwnership") if own is not None else None
            if dio:
                u = dio.strip().upper()
                if u.startswith("D"):
                    dio_norm = "D"
                elif u.startswith("I"):
                    dio_norm = "I"
                else:
                    dio_norm = dio.strip()
            else:
                dio_norm = None

            total: Decimal | None = None
            if shares is not None and price is not None and not fn_ids:
                total = shares * price
            elif fn_ids:
                total = None

            txs.append(
                ParsedTransaction(
                    transaction_index=idx,
                    transaction_category="non_derivative",
                    security_title=security_title,
                    transaction_date=tdate,
                    transaction_code=tcode,
                    acquired_disposed_code=ad_code,
                    shares=shares,
                    price_per_share=price,
                    total_value=total,
                    direct_indirect=dio_norm,
                    footnote_ids=fn_ids,
                )
            )
            idx += 1

    d_table = _first_local(od, "derivativeTable")
    if d_table is not None:
        for tx in d_table.xpath(".//*[local-name()='derivativeTransaction']"):
            security_title = _security_title_text(tx)
            tdate = _parse_date(_text_local(tx, "transactionDate"))
            coding = _first_local(tx, "transactionCoding")
            tcode = _text_local(coding, "transactionCode") if coding is not None else None
            _ta = _first_local(tx, "transactionAmounts")
            ad = _first_local(_ta if _ta is not None else tx, "transactionAcquiredDisposedCode")
            ad_code = None
            if ad is not None:
                v = _first_local(ad, "value")
                raw = (v.text or "").strip() if v is not None and v.text else (ad.text or "").strip()
                if raw in ("A", "D"):
                    ad_code = raw
                elif raw:
                    ad_code = raw[:1].upper()

            shares = _decimal_local(tx, ("transactionAmounts", "transactionShares"))
            price = _decimal_local(tx, ("transactionAmounts", "transactionPricePerShare"))
            fn_ids = _collect_amount_footnotes(tx)

            own = _first_local(tx, "ownershipNature")
            dio = _text_local(own, "directOrIndirectOwnership") if own is not None else None
            if dio:
                u = dio.strip().upper()
                if u.startswith("D"):
                    dio_norm = "D"
                elif u.startswith("I"):
                    dio_norm = "I"
                else:
                    dio_norm = dio.strip()
            else:
                dio_norm = None

            txs.append(
                ParsedTransaction(
                    transaction_index=idx,
                    transaction_category="derivative",
                    security_title=security_title,
                    transaction_date=tdate,
                    transaction_code=tcode,
                    acquired_disposed_code=ad_code,
                    shares=shares,
                    price_per_share=price,
                    total_value=_derivative_total_value(),
                    direct_indirect=dio_norm,
                    footnote_ids=fn_ids,
                )
            )
            idx += 1

    issuer_name = _text_local(issuer, "issuerName") if issuer is not None else None
    issuer_ticker = _text_local(issuer, "issuerTradingSymbol") if issuer is not None else None

    return ParsedFiling(
        document_type=doc_type_norm,
        is_amendment=doc_type_norm == "4/A",
        schema_version=schema_version,
        period_of_report=period,
        issuer_cik=issuer_cik,
        issuer_name=issuer_name,
        issuer_ticker=issuer_ticker,
        insider_cik=insider_cik,
        insider_name=insider_name,
        insider_title=insider_title,
        transactions=txs,
        parse_warnings=warnings,
    )


def parsed_at_now() -> datetime:
    return datetime.now(timezone.utc)
