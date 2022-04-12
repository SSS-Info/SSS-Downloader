@echo off
call env\Scripts\activate.bat
py main.py -nophotos
call env\Scripts\deactivate.bat