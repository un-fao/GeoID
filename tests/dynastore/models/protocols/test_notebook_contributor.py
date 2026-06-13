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

from dynastore.models.localization import LocalizedText
from dynastore.models.protocols.notebook_contributor import NotebookContributorProtocol
from dynastore.modules.notebooks.contribution import NotebookContribution


class _Contributor:
    def get_notebooks(self) -> list[NotebookContribution]:
        return [
            NotebookContribution(
                notebook_id="t",
                title=LocalizedText(en="T"),
                registered_by="t",
                notebook_content={},
            )
        ]


class _NonContributor:
    pass


def test_runtime_isinstance_accepts_contributor():
    assert isinstance(_Contributor(), NotebookContributorProtocol)


def test_runtime_isinstance_rejects_non_contributor():
    assert not isinstance(_NonContributor(), NotebookContributorProtocol)
