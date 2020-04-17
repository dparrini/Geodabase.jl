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

gdbvalue(::Type{T}, handle, col) where {T <: Union{Base.BitSigned, Base.BitUnsigned}} = convert(T, getIntegerByIndex(handle, col-1))
gdbvalue(::Type{T}, handle, col) where {T <: Union{Float16, Float32}} = convert(T, getFloatByIndex(handle, col-1))
gdbvalue(::Type{T}, handle, col) where {T <: Float64} = convert(T, getDoubleByIndex(handle, col-1))
#TODO: test returning a WeakRefString instead of calling `unsafe_string`
gdbvalue(::Type{T}, handle, col) where {T <: Union{AbstractString, String}} = getStringByIndex(handle, col-1)
# function gdbvalue(::Type{T}, handle, col) where {T}
#     error("Not implemented yet for "*string(T))
#     # blob = convert(Ptr{UInt8}, sqlite3_column_blob(handle, col))
#     # b = sqlite3_column_bytes(handle, col)
#     # buf = zeros(UInt8, b) # global const?
#     # unsafe_copyto!(pointer(buf), blob, b)
#     # r = sqldeserialize(buf)::T
#     # return r
# end


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
  type
  typestr::String
  db::Database
  len::Int32
  nullable::Bool
  # TODO: add a parent member (can point to Table, QueryObj, Row)
end


mutable struct QueryObj
  ref::Ptr{Cvoid}
  tbl::Table
  db::Database
end


mutable struct Row
  ref::Ptr{Cvoid}
  query::QueryObj
  tbl::Table
  db::Database
end


TypeNames = Dict(
    0 => "SmallInteger",
    1 => "Integer",
    2 => "Single",
    3 => "Double",
    4 => "String",
    5 => "Date",
    6 => "OID",
    7 => "Geometry",
    8 => "Blob",
    9 => "Raster",
   10 => "GUID",
   11 => "GlobalID",
   12 => "XML",
)


TypeSymbols = Dict(
    0 => Int16,
    1 => Int32,
    2 => Float32,
    3 => Float64,
    4 => String,
    5 => Integer,  # TODO: store date as integer or string or object?
    6 => String,   # TODO: OID
    7 => String,   # TODO: Geometry
    8 => String,   # TODO: Blob
    9 => String,   # TODO: Raster
   10 => String,   # TODO: GUID
   11 => String,   # TODO: GlobalID
   12 => String,   # TODO: XML
)


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


function getFieldAlias(fieldptr::Ptr{Cvoid})
  name = Vector{Cwchar_t}(undef, 256)

  len = ccall((:gdbfield_get_alias, libgeodb), Int32, 
                  (Ptr{Cvoid}, Ptr{Cwchar_t}, Int32), 
                  fieldptr, name, 255)

  return transcode(String, name[1:len])
end


function getFieldType(fieldptr::Ptr{Cvoid})
  return ccall((:gdbfield_get_type, libgeodb), Int32, 
                 (Ptr{Cvoid},), fieldptr,)
end


function getFieldLength(fieldptr::Ptr{Cvoid})
  return ccall((:gdbfield_get_length, libgeodb), Int32, 
                 (Ptr{Cvoid},), fieldptr,)
end


function getFieldIsNullable(fieldptr::Ptr{Cvoid})
  return ccall((:gdbfield_get_is_nullable, libgeodb), Int32, 
                 (Ptr{Cvoid},), fieldptr,)
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

        inttype = getFieldType(ptr)
        name = getFieldName(ptr)
        alias = getFieldAlias(ptr)
        typestr = TypeNames[inttype]
        type = TypeSymbols[inttype]
        len = getFieldLength(ptr)
        nullable = getFieldIsNullable(ptr)

        push!(fields, Field(ptr, name, alias, type, typestr, tbl.db, 
          len, nullable))
      end
    end
  end
  return fields
end


function searchTable(tbl::Table, subfields, whereClause)
  if tbl.ref != C_NULL
    query = openQuery(tbl)
    if query.ref != C_NULL
      # TODO: transcode subfields and whereClause
      wsubfields = transcode(Cwchar_t, subfields)
      wwhereClause = transcode(Cwchar_t, whereClause)
      ret = ccall((:gdbtable_search, libgeodb), Int32, 
                      (Ptr{Cvoid},Ptr{Cvoid},Ptr{Cwchar_t},Ptr{Cwchar_t}),
                      tbl.ref, query.ref, wsubfields, wwhereClause)
      if ret != 0
        closeQuery(query)
      end
    end
    return query
  end
  return Query(C_NULL, tbl, tbl.db)
end


# internal?
function openQuery(tbl::Table)
  query = QueryObj(C_NULL, tbl, tbl.db)
  ret = ccall((:gdbquery_create, libgeodb), Ptr{Cvoid}, ())
  # TODO: check if the query is successful and set a variable
  if ret != C_NULL
    query.ref = ret
  end
  return query
end


function closeQuery(query::QueryObj)
  if query.ref != C_NULL
    ret = ccall((:gdbquery_close, libgeodb), Int32, (Ptr{Cvoid},), query.ref)

    if ret == 0
      query.ref = C_NULL
    end
  end
end


function getQueryFieldType(query, index)
  ptr = ccall((:gdbquery_get_field, libgeodb), Ptr{Cvoid}, 
                (Ptr{Cvoid},Int32), query.ref, index)
  if ptr != C_NULL
    inttype = getFieldType(ptr)
    return TypeSymbols[inttype]
  end
  return Any
end


function getQueryFieldsCount(query)
  fcount = 0
  if query.ref != C_NULL
    fcount = ccall((:gdbquery_get_fields_count, libgeodb), Int32,
                    (Ptr{Cvoid},), query.ref)
  end
  return fcount
end


function getQueryFields(query)
  # TODO: refactor, similar to getTableFields
  fields = Vector{Field}(undef, 0)
  if query.ref != C_NULL
    fcount = getQueryFieldsCount(query)

    for ifield = 1:fcount
      ret = ccall((:gdbquery_get_field, libgeodb), Ptr{Cvoid}, 
                    (Ptr{Cvoid},Int32), query.ref, ifield-1)
      if ret != C_NULL
        ptr = ret

        inttype = getFieldType(ptr)
        name = getFieldName(ptr)
        alias = getFieldAlias(ptr)
        typestr = TypeNames[inttype]
        type = TypeSymbols[inttype]
        len = getFieldLength(ptr)
        nullable = getFieldIsNullable(ptr)

        push!(fields, Field(ptr, name, alias, type, typestr, query.db, 
          len, nullable))
      end
    end
  end
  return fields
end


function nextQuery(query::QueryObj)
  if query.ref != C_NULL
    ret = ccall((:gdbquery_next, libgeodb), Ptr{Cvoid}, (Ptr{Cvoid},), query.ref)
    # rows are deleted with the parent query
    if ret != C_NULL
      # create row type
      return Row(ret, query, query.tbl, query.db)
    end
    return Row(C_NULL, query, query.tbl, query.db)
  end
end


function getRowFieldType(row, index)
  ptr = ccall((:gdbrow_get_field, libgeodb), Ptr{Cvoid}, 
                (Ptr{Cvoid},Int32), row.ref, index)
  if ptr != C_NULL
    inttype = getFieldType(ptr)
    return TypeSymbols[inttype]
  end
  return Any
end


function getRowField(row, index)
  ptr = ccall((:gdbrow_get_field, libgeodb), Ptr{Cvoid}, 
                (Ptr{Cvoid},Int32), row.ref, index)
  if ptr != C_NULL
    inttype = getFieldType(ptr)
    name = getFieldName(ptr)
    alias = getFieldAlias(ptr)
    typestr = TypeNames[inttype]
    type = TypeSymbols[inttype]
    len = getFieldLength(ptr)
    nullable = getFieldIsNullable(ptr)

    return Field(ptr, name, alias, type, typestr, row.db, 
      len, nullable)
  end
  return nothing
end


function getRowFields(row)
  # TODO: refactor, similar to getTableFields
  fields = Vector{Field}(undef, 0)
  if row.ref != C_NULL
    fcount = ccall((:gdbrow_get_fields_count, libgeodb), Int32,
                    (Ptr{Cvoid},), row.ref)

    for ifield = 1:fcount
      ret = ccall((:gdbrow_get_field, libgeodb), Ptr{Cvoid}, 
                    (Ptr{Cvoid},Int32), row.ref, ifield-1)
      if ret != C_NULL
        ptr = ret

        inttype = getFieldType(ptr)
        name = getFieldName(ptr)
        alias = getFieldAlias(ptr)
        typestr = TypeNames[inttype]
        type = TypeSymbols[inttype]
        len = getFieldLength(ptr)
        nullable = getFieldIsNullable(ptr)

        push!(fields, Field(ptr, name, alias, type, typestr, row.db, 
          len, nullable))
      end
    end
  end
  return fields
end


function getShortByIndex(row, index)
  if row.ref != C_NULL
    return ccall((:gdbrow_get_short_by_index, libgeodb), Int16,
                 (Ptr{Cvoid},Int32), row.ref, index)
  end
  error("Null row reference.")
end


function getIntegerByIndex(row, index)
  if row.ref != C_NULL
    return ccall((:gdbrow_get_integer_by_index, libgeodb), Int32,
                 (Ptr{Cvoid},Int32), row.ref, index)
  end
  error("Null row reference.")
end


function getFloatByIndex(row, index)
  if row.ref != C_NULL
    return ccall((:gdbrow_get_float_by_index, libgeodb), Float32,
                 (Ptr{Cvoid},Int32), row.ref, index)
  end
  error("Null row reference.")
end


function getDoubleByIndex(row, index)
  if row.ref != C_NULL
    return ccall((:gdbrow_get_double_by_index, libgeodb), Float64,
                 (Ptr{Cvoid},Int32), row.ref, index)
  end
  error("Null row reference.")
end


function getStringByIndex(row, index)
  if row.ref != C_NULL
    retstr = ccall((:gdbrow_get_string_by_index, libgeodb), Ptr{Cwchar_t},
                 (Ptr{Cvoid},Int32), row.ref, index)
    len = ccall(:wcslen, Int32, (Cwstring,), retstr)
    # println(len)
    # println(retstr)
    newarr = unsafe_wrap(Vector{Cwchar_t}, retstr, len, own=true)
    return transcode(String, newarr)
  end
  error("Null row reference.")
end


function getIsNullByIndex(row, index)
  if row.ref != C_NULL
    retval = ccall((:gdbrow_is_null_by_index, libgeodb), UInt8,
                 (Ptr{Cvoid},Int32), row.ref, index)
    return convert(Bool, retval)
  end
  error("Null row reference.")
end


include("tables.jl")


end # module