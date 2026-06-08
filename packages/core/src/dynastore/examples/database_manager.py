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

from dynastore.extensions.protocols import ExtensionProtocol
from contextlib import asynccontextmanager 
from fastapi import FastAPI

# A mock database client for the example
class MockDBClient:
    def connect(self): print("DB connection opened. 🐘")
    def close(self): print("DB connection closed. 🐘")
class DatabaseManager(ExtensionProtocol):
    # No router attribute defined!
    
    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Creates a DB client and shares it in the app's state."""
        print("DatabaseManager starting up...")
        db_client = MockDBClient()
        db_client.connect()
        app.state.db_client = db_client # Inject the client into the app state
        try:
            yield
        finally:
            app.state.db_client.close()
            print("DatabaseManager shut down.")