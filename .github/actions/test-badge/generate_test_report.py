"""
Generates a test-results.json report from JUnit XML test results.

Used by CI to produce a fixture consumed by the web UI badge panel.
Locally, the static fixture shows "pending" status.
"""
import xml.etree.ElementTree as ET
import json
import os
import argparse
from datetime import datetime, timezone


def parse_junit(xml_path):
    """Parse a JUnit XML file and return summary stats."""
    if not os.path.exists(xml_path):
        return {"passed": 0, "failed": 0, "skipped": 0, "total": 0, "status": "pending"}

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        if root.tag == "testsuites":
            total = sum(int(ts.get("tests", 0)) for ts in root)
            failures = sum(
                int(ts.get("failures", 0)) + int(ts.get("errors", 0)) for ts in root
            )
            skipped = sum(int(ts.get("skipped", 0)) for ts in root)
        else:
            total = int(root.get("tests", 0))
            failures = int(root.get("failures", 0)) + int(root.get("errors", 0))
            skipped = int(root.get("skipped", 0))

        passed = total - failures - skipped
        status = "passed" if failures == 0 and total > 0 else ("failed" if failures > 0 else "pending")

        return {
            "passed": passed,
            "failed": failures,
            "skipped": skipped,
            "total": total,
            "status": status,
        }
    except Exception as e:
        print(f"Error parsing {xml_path}: {e}")
        return {"passed": 0, "failed": 0, "skipped": 0, "total": 0, "status": "error"}


def generate(unit_xml, integration_xml, output_path, commit="local", branch="main"):
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commit": commit[:8] if commit else "local",
        "branch": branch,
        "unit": parse_junit(unit_xml),
        "integration": parse_junit(integration_xml),
    }

    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Test report generated at {output_path}")
    print(f"  unit:        {report['unit']}")
    print(f"  integration: {report['integration']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test-results.json from JUnit XML files")
    parser.add_argument("--unit-xml", default="./unit-test-results.xml")
    parser.add_argument("--integration-xml", default="./test-results.xml")
    parser.add_argument("--output", default="test-results.json")
    parser.add_argument("--commit", default=os.environ.get("GITHUB_SHA", "local"))
    parser.add_argument("--branch", default=os.environ.get("GITHUB_REF_NAME", "main"))
    args = parser.parse_args()

    generate(args.unit_xml, args.integration_xml, args.output, args.commit, args.branch)
