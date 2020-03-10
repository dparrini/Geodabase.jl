module Geodatabase


if isfile(joinpath(dirname(@__FILE__),"..","deps","deps.jl"))
    include("../deps/deps.jl")
else
    error("Geodatabase.jl not properly installed. Please run Pkg.build(\"Geodatabase\")")
end


function __init__()
    julia_libdir = joinpath(dirname(first(filter(x -> occursin("libjulia", x), Libdl.dllist()))), "julia")
    julia_bindir = Sys.BINDIR
    libgeodb_libdir = dirname(libgeodb)
    libgeodb_bindir = joinpath(dirname(libgeodb), "..", "bin")
    pathsep = Sys.iswindows() ? ';' : ':'
    @static if Sys.isapple()
        global amplexe_env_var = ["DYLD_LIBRARY_PATH"]
        global amplexe_env_val = "$(julia_libdir)$(pathsep)$(get(ENV,"DYLD_LIBRARY_PATH",""))"
    elseif Sys.islinux()
        global amplexe_env_var = ["LD_LIBRARY_PATH"]
        global amplexe_env_val = "$(julia_libdir)$(pathsep)$(get(ENV,"LD_LIBRARY_PATH",""))"
    elseif Sys.iswindows()
        # for some reason windows sometimes needs Path instead of PATH
        global amplexe_env_var = ["PATH","Path","path"]
        global amplexe_env_val = "$(julia_bindir)$(pathsep)$(get(ENV,"PATH",""))"
    end

    # Still need this for AmplNLWriter to work until it uses amplexefun defined above
    # (amplexefun wraps the call to the binary and doesn't leave environment variables changed.)
    @static if Sys.isapple()
         ENV["DYLD_LIBRARY_PATH"] = string(get(ENV, "DYLD_LIBRARY_PATH", ""), ":", julia_libdir)
    elseif Sys.islinux()
         ENV["LD_LIBRARY_PATH"] = string(get(ENV, "LD_LIBRARY_PATH", ""), ":", julia_libdir, ":", libgeodb_libdir)
    end
end


mutable struct Database
    ref::Ptr{Cvoid}  # Reference to the internal data structure
    path::String
    opened::Bool
end


mutable struct Table
  ref::Ptr{Cvoid}
  name::String
  db::Database
  opened::Bool
end


mutable struct Field
  ref::Ptr{Cvoid}
  name::String
  alias::String
  type::String
  db::Database
  len::Int32
  nullable::Bool
end


function openDatabase(path::String)
  db = Database(C_NULL, path, false)
  ret = ccall((:gdb_create, libgeodb), Ptr{Cvoid}, ())

  if ret != C_NULL
    db.ref = ret
    ret = ccall((:gdb_open, libgeodb), Int32, 
      (Ptr{Cvoid},Cwstring), db.ref, db.path)

    if ret == 0
      db.opened = true
    else
      closeDatabase(db)
    end
  end
  return db
end


function closeDatabase(db::Database)
    if db.ref != C_NULL
        ccall((:gdb_close, libgeodb), Cvoid, (Ptr{Cvoid},), db.ref)
        db.ref = C_NULL
    end
end


function getTablesCount(db::Database)
    if db.ref != C_NULL
        return  ccall((:gdb_get_tables_count, libgeodb), Int32, (Ptr{Cvoid},), db.ref)
    end
    return 0
end


function getTableNames(db::Database)
  tables = Vector{String}(undef, 0)
  if db.ref != C_NULL
    tblcount = getTablesCount(db)
    if tblcount > 0

      if db.ref != C_NULL # Ptr{Cwchar_t}
        for i = 1:tblcount
          tablename = Vector{Cwchar_t}(undef, 256)
          len = ccall((:gdb_get_table_name, libgeodb), Int32, 
            (Ptr{Cvoid}, Int32, Ptr{Cwchar_t}, Int32), db.ref, i-1, tablename, 256)
          utablename = transcode(String, tablename[1:len])
          push!(tables, utablename)
        end
      end
    end
  end
  return tables
end


function openTable(db::Database, table::String)
  tbl = Table(C_NULL, table, db, false)
  ret = ccall((:gdbtable_create, libgeodb), Ptr{Cvoid}, ())

  if ret != C_NULL
    tbl.ref = ret
    ret = ccall((:gdbtable_open, libgeodb), Int32, 
      (Ptr{Cvoid},Ptr{Cvoid},Cwstring), tbl.ref, tbl.db.ref, tbl.name)

    if ret == 0
      tbl.opened = true
    else
      closeTable(tbl)
    end
  end
  return tbl
end


function closeTable(tbl::Table)
    if tbl.ref != C_NULL
        ccall((:gdbtable_close, libgeodb), Cvoid, 
          (Ptr{Cvoid},Ptr{Cvoid},), tbl.db.ref, tbl.ref)
        tbl.ref = C_NULL
    end
end


function getTableRowsCount(tbl::Table)
  if tbl.ref != C_NULL
    return ccall((:gdbtable_get_row_count, libgeodb), Int32, 
      (Ptr{Cvoid},), tbl.ref)
  end
  return 0
end


# """
# p = @cbc_ccall getVectorStarts Ptr{CoinBigIndex} (Ptr{Cvoid},) prob
#     num_cols = Int(getNumCols(prob))
#     return copy(unsafe_wrap(Array,p,(num_cols+1,)))
# """

function getFieldName(fieldptr::Ptr{Cvoid})
  name = Vector{Cwchar_t}(undef, 256)

  len = ccall((:gdbfield_get_name, libgeodb), Int32, 
                  (Ptr{Cvoid}, Ptr{Cwchar_t}, Int32), 
                  fieldptr, name, 255)

  return transcode(String, name[1:len])
end


function getTableFields(tbl)
  fields = Vector{Field}(undef, 0)
  if tbl.ref != C_NULL
    fcount = ccall((:gdbtable_get_fields_count, libgeodb), Int32,
                    (Ptr{Cvoid},), tbl.ref)

    for ifield = 1:fcount
      ret = ccall((:gdbtable_get_field, libgeodb), Ptr{Cvoid}, 
                    (Ptr{Cvoid},Int32), tbl.ref, ifield-1)
      if ret != C_NULL
        ptr = ret

        name = getFieldName(ptr)
        alias = "Field"
        type = "Type"
        len = 4
        nullable = true

        push!(fields, Field(ptr, name, alias, type, tbl.db, 
          len, nullable))
      end
    end
  end
  return fields
end

end # module