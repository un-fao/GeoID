#!/usr/bin/env python3
"""
Generate geoid-refs.json manifest with recent commits, tags, and test status.

Usage (in CI):
    python generate_refs_manifest.py --repo un-fao/GeoID --output geoid-refs.json
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone


def fetch_options(repo: str, commits: int = 5, tags: int = 3) -> list[str]:
    """Fetch recent commits and tags from GitHub API via gh CLI."""
    options = ["main"]

    # Fetch recent commits
    try:
        result = subprocess.run(
            [
                "gh", "api", f"repos/{repo}/commits",
                "-q", f'.[0:{commits}] | .[] | .sha[0:12] + " - " + (.commit.message | split("\\n")[0][0:60])',
            ],
            capture_output=True, text=True, check=True,
        )
        for line in result.stdout.strip().splitlines():
            if line.strip():
                options.append(line.strip())
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Warning: failed to fetch commits: {e}", file=sys.stderr)

    # Fetch recent tags
    try:
        result = subprocess.run(
            [
                "gh", "api", f"repos/{repo}/tags?per_page={tags}",
                "-q", '.[] | "tag:" + .name',
            ],
            capture_output=True, text=True, check=True,
        )
        for line in result.stdout.strip().splitlines():
            if line.strip():
                options.append(line.strip())
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Warning: failed to fetch tags: {e}", file=sys.stderr)

    return options


def get_head_sha() -> str:
    """Get the current HEAD commit SHA."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ""


def main():
    parser = argparse.ArgumentParser(description="Generate GeoID refs manifest")
    parser.add_argument("--repo", default="un-fao/GeoID", help="Source repo (owner/name)")
    parser.add_argument("--output", default="geoid-refs.json", help="Output file path")
    parser.add_argument("--commits", type=int, default=5, help="Number of recent commits")
    parser.add_argument("--tags", type=int, default=3, help="Number of recent tags")
    parser.add_argument("--tests-passed", default="true", help="Whether tests passed")
    parser.add_argument("--test-run-url", default="", help="URL to the test run")
    args = parser.parse_args()

    options = fetch_options(args.repo, args.commits, args.tags)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "head_sha": get_head_sha(),
        "tests_passed": args.tests_passed.lower() == "true",
        "test_run_url": args.test_run_url,
        "options": options,
    }

    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"Wrote {args.output} with {len(options)} options.")


if __name__ == "__main__":
    main()
