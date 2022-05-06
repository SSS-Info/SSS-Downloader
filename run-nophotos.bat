@echo off
call env\Scripts\activate.bat
py main.py -nophotos -pdf yes
call env\Scripts\deactivate.bat