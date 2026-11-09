"""Command groups for the bridge CLI (task #210 split).

Each module owns its ``_cmd_*`` handlers by domain;
``bridge.py`` keeps the central parser and the ``_HANDLERS``
dispatch table that references them.
"""
