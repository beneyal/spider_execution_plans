import subprocess
import sys

from pathlib import Path


sqls = Path("schemas").glob("*/*.sql")

for sql in sqls:
    subprocess.run(args=["sqlcmd", "-S", sys.argv[1], "-i", str(sql)])
