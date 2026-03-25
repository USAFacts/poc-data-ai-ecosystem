# JupyterLab configuration

c = get_config()  # noqa: F821

# Server settings
c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True

# Disable token authentication for development
c.ServerApp.token = ""
c.ServerApp.password = ""

# Allow CORS for development
c.ServerApp.allow_origin = "*"
c.ServerApp.disable_check_xsrf = True

# Set the default working directory (use /home/jovyan to see both work and examples)
c.ServerApp.root_dir = "/home/jovyan"

# Kernel settings
c.KernelSpecManager.ensure_native_kernel = True
