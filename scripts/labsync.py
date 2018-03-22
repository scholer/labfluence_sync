#!/usr/bin/python

import sys
import os
LIBDIR = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
sys.path.insert(0, LIBDIR)
print("\n".join(sys.path))

from labfluence_sync import syncmanager
from labfluence_sync.syncmanager import main


if __name__ == "__main__":
    main()

