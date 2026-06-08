#    Copyright 2026 FAO
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
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _dimensions_presets  # noqa: F401 -- preset registration side-effect

_examples_dir = Path(__file__).parent / "examples"

if _examples_dir.is_dir():
    for ipynb in sorted(_examples_dir.glob("*.ipynb")):
        register_platform_notebook(
            notebook_id=ipynb.stem,
            registered_by="dimensions_extension",
            notebook_path=ipynb,
        )
