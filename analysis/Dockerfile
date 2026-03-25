FROM quay.io/jupyter/scipy-notebook:python-3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# Copy package files
COPY --chown=${NB_UID}:${NB_GID} pyproject.toml /home/jovyan/package/
COPY --chown=${NB_UID}:${NB_GID} src/ /home/jovyan/package/src/

# Install the package
WORKDIR /home/jovyan/package
RUN pip install --no-cache-dir -e .

# Copy Jupyter configuration
COPY --chown=${NB_UID}:${NB_GID} jupyter_config.py /home/jovyan/.jupyter/jupyter_server_config.py

# Create work directory
WORKDIR /home/jovyan/work

# Expose JupyterLab port
EXPOSE 8888

# Start JupyterLab
CMD ["start-notebook.py", "--NotebookApp.token=''", "--NotebookApp.password=''"]
