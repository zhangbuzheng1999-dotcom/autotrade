from __future__ import annotations

import re

from ..utils.contract_config import Contract_info, FeeType


def extract_underlying_symbol(order_book_id: str) -> str:
    match = re.match(r"^([A-Z]+)", str(order_book_id).upper())
    if not match:
        raise ValueError(f"cannot extract underlying symbol from {order_book_id}")
    return match.group(1)


def get_contract_rule(order_book_id: str) -> dict | None:
    symbol = extract_underlying_symbol(order_book_id)
    return Contract_info.get(symbol)


def resolve_contract_rule_key(instrument_row: dict) -> str | None:
    asset_type = str(instrument_row.get("asset_type") or "").strip()
    underlying_symbol = instrument_row.get("underlying_symbol")
    order_book_id = instrument_row.get("order_book_id")

    if asset_type == "FUND" and underlying_symbol:
        return str(underlying_symbol).upper()

    if asset_type in {"Future", "Option", "Spot"}:
        if underlying_symbol:
            return str(underlying_symbol).upper()
        if order_book_id:
            return extract_underlying_symbol(str(order_book_id).upper())
        return None

    return None


def get_contract_rule_by_key(rule_key: str | None) -> dict | None:
    if not rule_key:
        return None
    return Contract_info.get(str(rule_key).upper())


def calc_trade_fee(*, order_book_id: str, qty: float, price: float) -> float:
    rule = get_contract_rule(order_book_id)
    if rule is None:
        return 0.0
    fee_type = rule.get("fee_type")
    fee_unit = float(rule.get("fee_unit", 0.0))
    contract_amt = float(rule.get("contract_amt", 1.0))
    qty = abs(float(qty))
    if fee_type == FeeType.fee_by_amt:
        return qty * fee_unit
    if fee_type == FeeType.fee_by_val:
        return qty * float(price) * contract_amt * fee_unit
    return 0.0


def get_margin_rate(order_book_id: str) -> float:
    rule = get_contract_rule(order_book_id)
    if rule is None:
        return 0.0
    return float(rule.get("margin_rate", 0.0))


def get_multiplier(order_book_id: str) -> float:
    rule = get_contract_rule(order_book_id)
    if rule is None:
        return 1.0
    return float(rule.get("contract_amt", 1.0))


def get_exchange(order_book_id: str) -> str | None:
    rule = get_contract_rule(order_book_id)
    if rule is None:
        return None
    return rule.get("exchange")


def calc_trade_fee_from_rule(*, rule: dict | None, qty: float, price: float) -> float:
    if rule is None:
        return 0.0
    fee_type = rule.get("fee_type")
    fee_unit = float(rule.get("fee_unit", 0.0))
    contract_amt = float(rule.get("contract_amt", 1.0))
    qty = abs(float(qty))
    if fee_type == FeeType.fee_by_amt:
        return qty * fee_unit
    if fee_type == FeeType.fee_by_val:
        return qty * float(price) * contract_amt * fee_unit
    return 0.0
