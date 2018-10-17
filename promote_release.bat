@echo off
rem ### CODE OWNERS: Ben Copeland, Chas Busenburg
rem 
rem ### OBJECTIVE:
rem   Run the promotion process to promote a new version of this component
rem 
rem ### DEVELOPER NOTES:
rem  *none*


rem LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling environment setup script for product component
call "%~dp0setup_env.bat"

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling promotion script
python -m pref_sens_surg.promotion
