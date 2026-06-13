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

from contextvars import ContextVar, Token

_correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)

_INTERNAL_KEY = "_request_correlation_id"


def get_correlation_id() -> str | None:
    return _correlation_id_var.get()


def set_correlation_id(cid: str) -> Token:
    return _correlation_id_var.set(cid)
