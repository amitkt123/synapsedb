@echo off
REM SynapseDB CLI Launch Script for Windows

echo ============================================================
echo              SynapseDB CLI Launcher
echo ============================================================
echo.

REM Check if Java is installed
where java >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: Java is not installed or not in PATH
    echo Please install Java 11 or higher
    pause
    exit /b 1
)

REM Determine the script directory
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

REM Check if JAR exists
set JAR_PATH=%SCRIPT_DIR%target\synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar

if not exist "%JAR_PATH%" (
    echo Building SynapseDB CLI...
    cd /d "%PROJECT_ROOT%"

    call mvn clean package -pl synapsedb-cli -am -DskipTests
    if %ERRORLEVEL% NEQ 0 (
        echo Build failed!
        pause
        exit /b 1
    )

    echo Build successful
    echo.
)

REM Check if JAR exists after build
if not exist "%JAR_PATH%" (
    echo Error: JAR file not found at %JAR_PATH%
    pause
    exit /b 1
)

REM Launch the CLI with any provided arguments
echo Launching SynapseDB CLI...
echo.

java -jar "%JAR_PATH%" %*

