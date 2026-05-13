"""Helper: exact text from Mudrex Insights TAO signal 2026-05-13 08:55:05.

Stored as a Python literal so neither shell escapes nor ssh round-trips
mangle the dollar signs. Imported by test_parser_mudrex_tao.py.
"""
MUDREX_TAO = "\n".join([
    "\U0001f6a8 NEW CRYPTO TRADE ALERT \U0001f4c9\U0001f525",
    "",
    "\U0001f539 TRADE: TAO SHORT",
    "\U0001f539 Pair: TAO/USDT",
    "\U0001f539 Risk: HIGH",
    "\U0001f539 Leverage: 3x",
    "\U0001f539 Risk Reward Ratio: 1:1",
    "\U0001f539 Potential Profit: 14.52%",
    "",
    "\U0001f570️ Holding time: 1–2 days",
    "",
    "\U0001f538 Entry: $310",
    "",
    "\U0001f3af Take Profit (TP) 1: $302",
    "\U0001f3af Take Profit (TP) 2: $295",
    "",
    "\U0001f6d1 Stop Loss (SL): $325",
])
