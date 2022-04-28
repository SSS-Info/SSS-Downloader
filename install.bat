ac@echo off
py --version
py -m pip install virtualenv
py -m venv env
call env\Scripts\activate.bat
py -m pip install -r requirements.txt
call env\Scripts\deactivate.bat