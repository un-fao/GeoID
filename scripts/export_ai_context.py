import os
import tarfile
from pathlib import Path

def export_context():
    tar_name = "dynastore_ai_context.tar.gz"
    root_dir = Path(__file__).parent.parent.resolve()
    
    print(f"Packaging AI context into {tar_name}...")
    exclude_dirs = {'.git', '.venv', '__pycache__', 'node_modules'}
    
    files_to_add = []
    
    # Traverse directories to find context markdown files
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for f in filenames:
            if f.endswith(".ai_context.md") or f == "AGENT.md" or f.lower().startswith("readme") and f.lower().endswith(".md"):
                file_path = Path(dirpath) / f
                files_to_add.append(file_path)
                
    with tarfile.open(root_dir / tar_name, "w:gz") as tar:
        # Add dynamic context markers
        for f in files_to_add:
            rel_path = f.relative_to(root_dir)
            tar.add(f, arcname=rel_path)
            
        # Add static architecture constraints
        static_dirs = [".agent", ".memory", ".antigravity", "skills"]
        for dir_name in static_dirs:
            dir_path = root_dir / dir_name
            if dir_path.exists():
                tar.add(dir_path, arcname=dir_name)
                
    print(f"Done. Distribute {tar_name}.")
    print("Extract in repository root.")

if __name__ == "__main__":
    export_context()
