import subprocess
import sys


def test_locating_config_package_does_not_import_ccflow():
    # lerna resolves pkg://ccflow_s3.config on every hydra app startup
    code = "import importlib.resources, sys; importlib.resources.files('ccflow_s3.config'); assert 'ccflow' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
