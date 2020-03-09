## This file autogenerated by BinaryProvider.write_deps_file().
## Do not edit.
##
## Include this file within your main top-level source, and call
## `check_deps()` from within your module's `__init__()` method

import Libdl

const libgeodb = joinpath(dirname(@__FILE__), "usr\\bin\\gdbi.dll")
const libfilegdbapi = joinpath(dirname(@__FILE__), "usr\\bin\\FileGDBAPID.dll")
function check_deps()
    global libgeodb
    if !isfile(libgeodb)
        error("$(libgeodb) does not exist, Please re-run Pkg.build(\"Geodatabase\"), and restart Julia.")
    end

    if Libdl.dlopen_e(libgeodb) in (C_NULL, nothing)
        error("$(libgeodb) cannot be opened, Please re-run Pkg.build(\"Geodatabase\"), and restart Julia.")
    end

    global libfilegdbapi
    if !isfile(libfilegdbapi)
        error("$(libfilegdbapi) does not exist, Please re-run Pkg.build(\"Geodatabase\"), and restart Julia.")
    end
end