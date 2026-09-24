# Charger dans le terminal courant : . ./scripts/spark-env.ps1
$projectRoot = Split-Path $PSScriptRoot -Parent
$javaExe = Get-ChildItem -Path (Join-Path $projectRoot '.runtime/java/*/bin/java.exe') | Select-Object -First 1
if (-not $javaExe) { throw 'Java local absent dans .runtime/java' }
$fileSystem = New-Object -ComObject Scripting.FileSystemObject
$shortRoot = $fileSystem.GetFolder($projectRoot).ShortPath
$env:JAVA_HOME = $fileSystem.GetFolder((Split-Path (Split-Path $javaExe.FullName -Parent) -Parent)).ShortPath
$env:SPARK_HOME = $fileSystem.GetFolder((Join-Path $projectRoot '.venv/Lib/site-packages/pyspark')).ShortPath
$env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
$env:PYSPARK_PYTHON = $fileSystem.GetFile((Join-Path $projectRoot '.venv/Scripts/python.exe')).ShortPath
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:UV_LINK_MODE = 'copy'

# Dossier temporaire isolé, avec chemin court pour les scripts Windows Spark.
$runtimeTemp = Join-Path $projectRoot ('.runtime/temp-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force $runtimeTemp | Out-Null
$env:TEMP = $fileSystem.GetFolder($runtimeTemp).ShortPath
$env:TMP = $env:TEMP
