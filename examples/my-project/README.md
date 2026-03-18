# my-project

A downstream project built on the [dynastore](https://github.com/un-fao/dynastore) framework.

## Structure

```
my-project/
├── src/
│   └── my_project/
│       ├── __init__.py
│       ├── main.py          # Re-exports dynastore app (or extends it)
│       ├── main_task.py     # One-off task entrypoint (Cloud Run Job)
│       ├── modules/         # Custom modules (extend dynastore modules)
│       ├── extensions/      # Custom extensions (extend dynastore extensions)
│       └── tasks/           # Custom tasks (extend dynastore tasks)
├── docker/
│   ├── Dockerfile           # Extends dynastore Dockerfile
│   ├── docker-compose.yml   # Local dev/prod compose
│   └── scripts/             # Inherited from dynastore (symlink or copy)
│       ├── start.sh         # Service entrypoint (api|worker)
│       └── start-task.sh    # Job entrypoint
├── .github/
│   ├── workflows/
│   │   └── deploy.yml       # Deployment pipeline (private)
│   └── config/
│       ├── apps.yml         # App matrix (services, workers, jobs)
│       ├── iac.yml          # Cloud Run Service spec
│       ├── iac-worker.yml   # Cloud Run Worker spec
│       └── iac-job.yml      # Cloud Run Job spec
├── setup.py                 # Declares dynastore as a dependency
└── pyproject.toml
```

## Getting Started

1. Install dynastore as a dependency in `setup.py` or `pyproject.toml`:
   ```toml
   [project]
   dependencies = ["dynastore"]
   ```

2. In `src/my_project/main.py`, import and extend the dynastore app:
   ```python
   from dynastore.main import app  # Re-use the FastAPI app
   # Add your custom routes, middleware, etc.
   ```

3. Copy (or symlink) `docker/scripts/` from dynastore, or reference the
   scripts directly from the dynastore package in your Dockerfile.

4. Use the same `deploy.yml` pipeline pattern — just update `apps.yml`
   to point to your services/workers/jobs.
