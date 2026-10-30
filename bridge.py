#!/usr/bin/env python3
"""Repo-root entry point: run the bridge CLI from a source checkout.

Equivalent to the installed ``bridge`` console script; prepends
``src/`` to ``sys.path`` so no install is required:

    ./bridge.py status --json
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from session_bridge.cli.bridge import main

if __name__ == "__main__":
    sys.exit(main())
