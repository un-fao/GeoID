import xml.etree.ElementTree as ET
import os
import argparse

def generate(input_path, output_path, label="tests"):
    passed, skipped = 0, 0
    
    if os.path.exists(input_path):
        try:
            tree = ET.parse(input_path)
            root = tree.getroot()
            # Handle <testsuites> or <testsuite> root tags
            if root.tag == 'testsuites':
                passed = sum(int(ts.get('tests', 0)) - int(ts.get('failures', 0)) - int(ts.get('errors', 0)) for ts in root)
                skipped = sum(int(ts.get('skipped', 0)) for ts in root)
            else:
                passed = int(root.get('tests', 0)) - int(root.get('failures', 0)) - int(root.get('errors', 0))
                skipped = int(root.get('skipped', 0))
        except Exception as e:
            print(f"Error parsing XML: {e}")

    # Simplified SVG Template
    # We use a bit more width if the label is long, but keeping it simple for now.
    label_width = max(40, len(label) * 8)
    total_width = label_width + 120
    
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20">
      <rect width="{label_width}" height="20" fill="#555"/>
      <rect x="{label_width}" width="120" height="20" fill="#4c1"/>
      <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,sans-serif" font-size="11">
        <text x="{label_width/2}" y="14">{label}</text>
        <text x="{label_width + 60}" y="14">{passed} passed, {skipped} skipped</text>
      </g>
    </svg>'''
    
    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(svg)
    print(f"Badge generated at {output_path} with label '{label}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--label", default="tests")
    args = parser.parse_args()
    generate(args.input, args.output, args.label)