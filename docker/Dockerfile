# STAGE 1: Builder (Production dependencies)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.4 AS builder

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

# Install Production Dependencies
COPY pyproject.toml setup.py LICENSE README.md requirements*.txt pytest.ini* setup.cfg* .coveragerc* ./

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
COPY .github/scripts/start.sh /usr/local/bin/start.sh
COPY .github/scripts/start-task.sh /usr/local/bin/start-task.sh
COPY .github/scripts/start-worker.sh /usr/local/bin/start-worker.sh
RUN chmod +x /usr/local/bin/start.sh /usr/local/bin/start-task.sh /usr/local/bin/start-worker.sh

# Use buildkit cache for pip
# These ARGs are used by setup.py wildcard discovery
ARG DYNASTORE_MODULES="*"
ARG DYNASTORE_EXTENSION_MODULES="*"
ARG DYNASTORE_TASK_MODULES="*"

# Install dependencies.
RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install --upgrade pip setuptools wheel && \
    DYNASTORE_MODULES="${DYNASTORE_MODULES}" \
    DYNASTORE_EXTENSION_MODULES="${DYNASTORE_EXTENSION_MODULES}" \
    DYNASTORE_TASK_MODULES="${DYNASTORE_TASK_MODULES}" \
    ${VENV_PATH}/bin/pip install ".[dynastore-enabled-modules]" uvicorn gunicorn

EXPOSE 80

ENTRYPOINT ["/usr/local/bin/start.sh"]

# STAGE 2: Tester (Inherit from builder and add test/dev deps)
FROM builder AS tester

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install ".[test,dev]"

# STAGE 3: Runtime (Production image)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.4 AS runtime

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
COPY --from=builder /usr/local/bin/start-worker.sh /usr/local/bin/start-worker.sh

# Ownership
RUN chown -R $APP_USER:$APP_USER ${APP_DIR} ${VENV_PATH}

USER $APP_USER

ENTRYPOINT ["/usr/local/bin/start.sh"]