#!/usr/bin/env python
import os
import sys
import subprocess
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Usage: python env_run.py <command> [args...]")
        sys.exit(1)

    # Load .env manually (without requiring python -m dotenv)
    # Simple parser supporting KEY=VALUE lines and ignoring comments
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            # Allow quoted values
            if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                value = value[1:-1]
            os.environ[key] = value

    # Ensure dbt uses the project-local profiles.yml
    os.environ.setdefault("DBT_PROFILES_DIR", str(Path(__file__).parent))

    # Run the provided command with the enriched environment
    cmd = sys.argv[1:]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
