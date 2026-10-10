@echo off

echo ==========================================
echo   TEST EXECUTION - HEALTH COLOMBIA
echo ==========================================

if "%1"=="" goto :usage
if "%1"=="unit" goto :unit
if "%1"=="integration" goto :integration
if "%1"=="data" goto :data
if "%1"=="all" goto :all
goto :usage

:unit
echo Running unit tests...
python -m pytest tests/ -v -m "unit" --tb=short
goto :end

:integration
echo Running integration tests...
python -m pytest tests/ -v -m "integration" --tb=short
goto :end

:data
echo Running data validation tests...
python -m pytest tests/ -v -m "data" --tb=short
goto :end

:all
echo Running ALL tests...
python -m pytest tests/ -v --tb=short
goto :end

:usage
echo.
echo Usage: run_tests.bat [option]
echo.
echo Options:
echo   unit         Run unit tests
echo   integration  Run integration tests
echo   data         Run data validation tests
echo   all          Run all tests
echo.
echo Example: run_tests.bat unit
goto :end

:end
echo.
echo ==========================================
echo   TESTS COMPLETED
echo ==========================================
