$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot
py -m pip install -r requirements.txt
py soholms_backend.py
