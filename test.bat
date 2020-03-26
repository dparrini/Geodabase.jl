@ECHO off
set build=release

del D:\repositorio\Geodatabase.jl\deps\usr\bin\*.dll >NUL

if "%build%"=="release" (
  copy /Y D:\repositorio\gdb-c-interface\build\FileGDBAPI.dll D:\repositorio\Geodatabase.jl\deps\usr\bin\         >NUL
  copy /Y D:\repositorio\gdb-c-interface\build\out\Release\gdbi.dll D:\repositorio\Geodatabase.jl\deps\usr\bin\   >NUL
) else (
  copy /Y D:\repositorio\gdb-c-interface\build\FileGDBAPID.dll D:\repositorio\Geodatabase.jl\deps\usr\bin\        >NUL
  copy /Y D:\repositorio\gdb-c-interface\build\out\Debug\gdbi.dll D:\repositorio\Geodatabase.jl\deps\usr\bin\     >NUL
)


cd .\src\
julia example.jl
cd .\..\