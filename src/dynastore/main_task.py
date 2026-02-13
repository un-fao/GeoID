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
#    See the License for the specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/
import asyncio
import logging
import argparse
import json
import traceback
from types import SimpleNamespace
import sys
import os

# --- Basic Configuration ---
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

async def report_failure(task_id: str, schema: str, error_message: str):
    """
    Attempts to report a fatal failure to the database.
    This helper initializes just enough of the module system to update the task status.
    """
    if not task_id or not schema:
        logger.error("Cannot report failure to DB: task_id or schema missing.")
        return

    try:
        from dynastore import modules
        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskUpdate, TaskStatusEnum
        from dynastore.bootstrap import bootstrap_foundation, instantiate_foundation
        
        app_state = SimpleNamespace()
        # Initialize foundational modules (db_config, db)
        bootstrap_foundation(modules_list=["db_config", "datastore", "catalog"])
        instantiate_foundation(app_state, modules_list=["db_config", "datastore", "catalog"])
        
        async with modules.lifespan(app_state, enabled_modules=["db_config", "datastore", "catalog"]):
            logger.info(f"Reporting failure for task '{task_id}' in schema '{schema}'...")
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import DatabaseProtocol
            db = get_protocol(DatabaseProtocol)
            if not db:
                raise RuntimeError("DatabaseProtocol implementation not found.")
            engine = db.engine
            
            update_data = TaskUpdate(
                status=TaskStatusEnum.FAILED,
                error_message=f"Fatal Runner Error: {error_message}"
            )
            import uuid
            await tasks_module.update_task(engine, uuid.UUID(task_id), update_data, schema=schema)
            logger.info(f"Successfully reported failure for task '{task_id}'.")
    except Exception as e:
        logger.critical(f"Failed to report failure to database: {e}", exc_info=True)

async def main(task_name: str, payload: dict, schema: str):
    """
    Generic framework to initialize foundational modules and run a single background task.
    This entrypoint does not load or manage web extensions.
    """
    from dynastore import modules, tasks
    from dynastore.tasks.bootstrap import bootstrap_task_env
    from pydantic import ValidationError
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.models import TaskUpdate, TaskStatusEnum
    import uuid

    # Create a simple app_state object. The lifespan managers will populate it.
    app_state = SimpleNamespace()
    task_id = payload.get("task_id")

    # 1. Bootstrap the environment
    bootstrap_task_env(app_state)

    # 2. Execute the module lifecycles to initialize them and attach to the app_state.
    async with modules.lifespan(app_state):
        try:
            logger.info("--- [main_task.py] Foundational Modules are active. ---")

            # 3. Execute the task lifecycles, which instantiates tasks with the now-populated app_state.
            async with tasks.manage_tasks(app_state):
                logger.info("--- [main_task.py] Background Tasks are active. ---")
                
                task_config = tasks.get_all_task_configs().get(task_name)
                if not task_config or not task_config.instance:
                    raise ValueError(f"Task '{task_name}' not found or failed to initialize.")
                target_task = task_config.instance
                
                logging.info(f"--- [main_task.py] Loaded task '{task_name}' successfully. ---")

                # Introspect the `run` method's type hints to find the expected payload model.
                payload_model = None
                run_method = getattr(target_task, 'run', None)
                if run_method and 'payload' in run_method.__annotations__:
                    payload_model = run_method.__annotations__['payload']
                if not payload_model:
                    raise TypeError(f"Task '{task_name}' has a `run` method without a 'payload' type annotation.")
                
                logging.info(f"--- [main_task.py] Task '{task_name}' expects payload of type '{payload_model.__name__}'. ---")
                logging.debug(f"--- [main_task.py] Payload: {payload} ---")
                # Validate the incoming dictionary against the task's expected payload model.
                validate_payload = payload_model.model_validate(payload)

                logger.info(f"--- [main_task.py] Executing task: '{task_name}' ---")
                res = await target_task.run(payload=validate_payload)
                
                logger.info(f"--- [main_task.py] Task '{task_name}' returned: {res} ---")
        except Exception as e:
            if task_id:
                logger.info(f"Attempting to report task failure using active lifecycle for task '{task_id}'...")
                from dynastore.modules import get_protocol
                from dynastore.models.protocols import DatabaseProtocol
                try:
                    db = get_protocol(DatabaseProtocol)
                    if not db:
                        raise RuntimeError("DatabaseProtocol implementation not found.")
                    engine = db.engine
                    update_data = TaskUpdate(
                        status=TaskStatusEnum.FAILED,
                        error_message=f"Runtime Error: {str(e)}"
                    )
                    await tasks_module.update_task(engine, uuid.UUID(task_id), update_data, schema=schema)
                    logger.info("Successfully reported failure to DB.")
                except Exception as report_error:
                    logger.error(f"Failed to report failure within lifecycle: {report_error}. Falling back to external reporter.")
            raise

    logger.info(f"--- [main_task.py] Task '{task_name}' execution complete. All lifecycles shut down. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DynaStore Generic Task Runner.")
    parser.add_argument("task_name", type=str, help="The registered name of the task to run.")
    parser.add_argument("payload", type=str, help="The JSON payload for the task as a string.")
    parser.add_argument("--schema", type=str, help="The database schema where the task is registered.", default="tasks")
    args = parser.parse_args()
    
    task_id = None
    try:
        payload_dict = json.loads(args.payload)
        task_id = payload_dict.get("task_id")
        asyncio.run(main(args.task_name, payload_dict, args.schema))
    except (json.JSONDecodeError) as e:
        msg = f"Fatal: Invalid JSON payload for task '{args.task_name}'.\n{e}"
        logger.critical(msg, exc_info=True)
        # We probably don't have a task_id if JSON is invalid, so just exit
        sys.exit(1)
    except Exception as e:
        full_error = f"{e}\n{traceback.format_exc()}"
        logger.critical(f"An unexpected fatal error occurred: {full_error}")
        if task_id:
            asyncio.run(report_failure(task_id, args.schema, str(e)))
        sys.exit(1)
