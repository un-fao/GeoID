# STAGE 0: JupyterLite Builder — pre-builds browser-based Python kernel (Pyodide)
FROM python:3.12-slim AS jupyterlite-builder

WORKDIR /build

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install jupyterlite-core jupyterlite-pyodide-kernel

COPY src/dynastore/extensions/notebooks/jupyterlite/ ./lite-config/

RUN jupyter lite build \
    --lite-dir ./lite-config \
    --output-dir ./lite-output

# STAGE 1: Builder (Production dependencies)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.3 AS builder

LABEL maintainer="carlo.cancellieri@fao.org"

ARG APP_DIR=/dynastore
ARG VENV_PATH=/opt/venv

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git build-essential libpq-dev python3-full python3-pip \
    python3.12-venv python3.12-dev sudo curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${APP_DIR}

# Virtual Env Setup
RUN python3.12 -m venv ${VENV_PATH}
ENV PATH="${VENV_PATH}/bin:$PATH"
ENV PYTHONPATH=${APP_DIR}/src

# Build-time commit hash for version info (no .git in runtime image)
ARG BUILD_COMMIT=unknown
ENV BUILD_COMMIT=${BUILD_COMMIT}

# Install Production Dependencies
COPY pyproject.toml LICENSE README.md requirements*.txt pytest.ini* setup.cfg* .coveragerc* VERSION ./

# Copy .env file if it exists (CI creates it in root, local has it in env/)
# We use pyproject.toml as a sentinel to prevent COPY from failing if .env is missing.
RUN mkdir -p ./env
COPY pyproject.toml .en[v]* ./env/
# Also copy to root and src to be safe for discovery logic
COPY pyproject.toml .en[v]* ./
COPY pyproject.toml .en[v]* ./src/
COPY src ./src
COPY tests ./tests

# Copy startup scripts early so they are available in all stages
COPY docker/scripts/start.sh /usr/local/bin/start.sh
COPY docker/scripts/start-task.sh /usr/local/bin/start-task.sh
RUN chmod +x /usr/local/bin/start.sh /usr/local/bin/start-task.sh
# SCOPE selects which pyproject.toml extras to install.
# Set per-service via docker-compose.yml build args (from .env) or CI.
ARG SCOPE=all
# Set BUILD_JUPYTERLITE=false for services that do not include the notebooks scope (e.g. worker).
ARG BUILD_JUPYTERLITE=false
# Use buildkit cache for pip
RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install --upgrade pip setuptools wheel && \
    ${VENV_PATH}/bin/pip install ".[${SCOPE}]" uvicorn gunicorn

# Overlay JupyterLite assets when scope includes notebooks; bridge.js (committed) is preserved
RUN --mount=type=bind,from=jupyterlite-builder,source=/build/lite-output,target=/tmp/lite-output \
    if [ "${BUILD_JUPYTERLITE}" = "true" ]; then \
        cp -r /tmp/lite-output/. "${APP_DIR}/src/dynastore/extensions/notebooks/static/lite/" && \
        echo "JupyterLite: assets installed"; \
    else \
        echo "JupyterLite: skipped (BUILD_JUPYTERLITE=false)"; \
    fi

EXPOSE 80

# Default: start as API service. Override CMD to "worker" for worker services.
ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]

# STAGE 2: Tester (Inherit from builder and add test/dev deps)
FROM builder AS tester

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

ARG SCOPE=test,all
RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install ".[${SCOPE},dev]"

# STAGE 3: Development (Specific for local development with debugging)
FROM tester AS development

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

# debugpy is already installed via .[dev] in tester stage
EXPOSE 5678 5679

# STAGE 4: Runtime (Production image)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.3 AS runtime

ARG APP_USER=dynastore
ARG APP_UID=1001
ARG APP_GID=1001
ARG APP_DIR=/dynastore
ENV VENV_PATH=/opt/venv

# Install runtime dependencies (curl for healthchecks)
RUN apt-get update && \
    apt-get install -y --no-install-recommends sudo curl && \
    rm -rf /var/lib/apt/lists/*

# Environment setup
ENV APP_DIR=${APP_DIR}
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=${APP_DIR}/src
ENV PATH="${VENV_PATH}/bin:${HOME}/.local/bin:$PATH"
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# User Creation
RUN groupadd --gid $APP_GID $APP_USER && \
    useradd --uid $APP_UID --gid $APP_GID --create-home --shell /bin/bash $APP_USER

WORKDIR ${APP_DIR}

# Copy venv and source from builder (PROD ONLY)
COPY --from=builder ${VENV_PATH} ${VENV_PATH}
COPY --from=builder ${APP_DIR} ${APP_DIR}
# Scripts are already in /usr/local/bin from builder stage if we inherit,
# but runtime inherits from gdal, so we still need to copy them from builder.
COPY --from=builder /usr/local/bin/start.sh /usr/local/bin/start.sh
COPY --from=builder /usr/local/bin/start-task.sh /usr/local/bin/start-task.sh

# Ownership
RUN chown -R $APP_USER:$APP_USER ${APP_DIR} ${VENV_PATH}

USER $APP_USER

# Default: start as API service. Override CMD to "worker" for worker services.
# For Cloud Run Jobs, use start-task.sh as the entrypoint in the job spec.
ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]