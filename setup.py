#    Copyright 2025 FAO
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the a specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import os
import sys
from setuptools import setup, find_packages
import logging
from typing import Set, Dict, List

try:
    import tomllib
except ImportError:
    import tomli as tomllib

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', stream=sys.stdout)

# The project root is the directory containing this setup.py file.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# --- Parse Requirements Fallback ---
# We define a robust parser here to avoid dependency on dynastore.tools during setup
def parse_requirements(file_path: str, processed_files: Set[str] = None) -> Set[str]:
    if processed_files is None:
        processed_files = set()
    
    # Expand variables in the file path itself (e.g. ${APP_DIR})
    expanded_file_path = os.path.expandvars(file_path)
    if not os.path.isabs(expanded_file_path):
        expanded_file_path = os.path.join(PROJECT_ROOT, expanded_file_path)
    
    if expanded_file_path in processed_files:
        return set()
    processed_files.add(expanded_file_path)

    if not os.path.exists(expanded_file_path):
        return set()

    packages = set()
    try:
        with open(expanded_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                processed_line = os.path.expandvars(line.strip())
                if not processed_line or processed_line.startswith('#'):
                    continue
                if processed_line.startswith('-r'):
                    _, next_file = processed_line.split(maxsplit=1)
                    next_file_path = os.path.join(os.path.dirname(expanded_file_path), next_file)
                    packages.update(parse_requirements(next_file_path, processed_files))
                else:
                    packages.add(processed_line)
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")
    return packages


# --- Set APP_DIR environment variable ---
# This makes it available for substitution in requirements files (e.g., ${APP_DIR}).
os.environ['APP_DIR'] = PROJECT_ROOT

APP = os.getenv("APP", "dynastore")

# (Function replaced by import from dynastore.tools.dependencies)

def process_component_dependencies(
    env_var: str,
    base_dir: str,
    extras_dict: Dict[str, List[str]],
    all_packages_set: Set[str]
):
    """
    Reusable function to parse dependencies for modules, extensions, or tasks.
    
    It checks two levels of requirements:
    1. Category Level: src/APP/{base_dir}/requirements.txt (Installed if ANY component in this category is enabled)
    2. Component Level: src/APP/{base_dir}/{component}/requirements.txt (Installed if that specific component is enabled)
    """
    components_str = os.environ.get(env_var, "")
    
    # If the environment variable is not set or empty, we skip everything for this category.
    if not components_str:
        logging.info(f"'{env_var}' not set. No dependencies will be processed for this type.")
        return

    component_root = os.path.join(PROJECT_ROOT, "src", APP, base_dir)

    if components_str.strip() == "*":
        if os.path.isdir(component_root):
            components = [
                item for item in os.listdir(component_root)
                if os.path.isdir(os.path.join(component_root, item))
            ]
            logging.info(f"Wildcard '*' detected for '{env_var}'. Discovered components: {components}")
        else:
            logging.warning(f"Category directory not found for wildcard: {component_root}")
            return
    else:
        components = [c.strip() for c in components_str.split(',') if c.strip()]
    
    # If no valid components resulted from the split, return.
    if not components:
        return

    logging.info(f"Processing dependencies for components in '{env_var}': {components}")

    # --- 1. Process Category-Level Requirements ---
    # Example: src/dynastore/extensions/requirements.txt
    # This file is included if AT LEAST ONE component of this type is enabled.
    category_req_path = os.path.join("src", APP, base_dir, "requirements.txt")
    abs_category_req_path = os.path.join(PROJECT_ROOT, category_req_path)
    
    if os.path.exists(abs_category_req_path):
        logging.info(f"Found generic requirements for '{base_dir}': {abs_category_req_path}")
        category_packages = parse_requirements(abs_category_req_path)
        all_packages_set.update(category_packages)
        # We can also add these to a category-specific extra if desired, e.g. "extensions_common"
        extras_dict[f"{base_dir}_common"] = sorted(list(category_packages))

    # --- 2. Process Component-Level Requirements ---
    for component_name in components:
        # Construct path relative to the project root
        relative_path = os.path.join("src", APP, base_dir, component_name, "requirements.txt")
        absolute_file_path = os.path.join(PROJECT_ROOT, relative_path)
        
        component_packages = parse_requirements(absolute_file_path)

        extras_dict[component_name] = sorted(list(component_packages))
        all_packages_set.update(component_packages)

def discover_and_process_all_components(
    base_dir: str,
    extras_dict: Dict[str, List[str]],
    all_packages_set: Set[str]
):
    """Discovers all components in a directory and processes their dependencies."""
    component_root = os.path.join(PROJECT_ROOT, "src", APP, base_dir)
    if not os.path.isdir(component_root):
        return

    all_components = [
        item for item in os.listdir(component_root)
        if os.path.isdir(os.path.join(component_root, item))
    ]
    
    for component_name in all_components:
        relative_path = os.path.join("src", APP, base_dir, component_name, "requirements.txt")
        absolute_file_path = os.path.join(PROJECT_ROOT, relative_path)
        component_packages = parse_requirements(absolute_file_path)
        all_packages_set.update(component_packages)

def build_extras() -> Dict[str, List[str]]:
    """Builds the extras_require dictionary from .txt files for all component types."""
    extras_require: Dict[str, List[str]] = {}
    env_based_packages: Set[str] = set()
    all_discovered_packages: Set[str] = set()
    
    # 1. Load static extras from requirements files as the base
    extras_require['test'] = sorted(list(parse_requirements(os.path.join(PROJECT_ROOT, 'requirements-test.txt'))))
    extras_require['dev'] = sorted(list(parse_requirements(os.path.join(PROJECT_ROOT, 'requirements-dev.txt'))))
    logging.info(f"Loaded static extras: {list(extras_require.keys())}")

    # 2. Process dependencies based on environment variables for selective installs
    process_component_dependencies("DYNASTORE_MODULES", "modules", extras_require, env_based_packages)
    process_component_dependencies("DYNASTORE_EXTENSION_MODULES", "extensions", extras_require, env_based_packages)
    process_component_dependencies("DYNASTORE_TASK_MODULES", "tasks", extras_require, env_based_packages)

    enabled_extra_name = "dynastore-enabled-modules"
    extras_require[enabled_extra_name] = sorted(list(env_based_packages))

    # 3. Discover ALL components to build the 'all' extra
    discover_and_process_all_components("modules", extras_require, all_discovered_packages)
    discover_and_process_all_components("extensions", extras_require, all_discovered_packages)
    discover_and_process_all_components("tasks", extras_require, all_discovered_packages)

    # 4. Create the 'all' extra that includes test, dev, and ALL discovered modules
    all_deps = set()
    all_deps.update(extras_require.get('test', []))
    all_deps.update(extras_require.get('dev', []))
    all_deps.update(all_discovered_packages)
    extras_require['all'] = sorted(list(all_deps))

    logging.info("-" * 40)
    logging.info("Setup complete. The following extras will be available:")
    for extra, pkgs in extras_require.items():
        if pkgs:
            logging.info(f"  - {extra}: {pkgs}")
    logging.info(f"To install all enabled modules, run: pip install .[{enabled_extra_name}]")
    logging.info("-" * 40)

    return extras_require

setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    extras_require=build_extras()
)