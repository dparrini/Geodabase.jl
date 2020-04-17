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

    @static if Sys.isapple()
         ENV["DYLD_LIBRARY_PATH"] = string(get(ENV, "DYLD_LIBRARY_PATH", ""), ":", julia_libdir)
    elseif Sys.islinux()
         ENV["LD_LIBRARY_PATH"] = string(get(ENV, "LD_LIBRARY_PATH", ""), ":", julia_libdir, ":", libgeodb_libdir)
    end
end


gdbvalue(T::Symbol, handle, col)     = gdbvalue(Val{T}(), handle, col)
gdbvalue(T::Val{:SmallInteger}, handle, col) = getShortByIndex(handle, col-1)
gdbvalue(T::Val{:Integer}, handle, col)      = getIntegerByIndex(handle, col-1)
gdbvalue(T::Val{:Single}, handle, col)       = getFloatByIndex(handle, col-1)
gdbvalue(T::Val{:Double}, handle, col)       = getDoubleByIndex(handle, col-1)
gdbvalue(T::Val{:String}, handle, col)       = getStringByIndex(handle, col-1)
gdbvalue(T::Val{:Date}, handle, col)         = getDateByIndex(handle, col-1)
gdbvalue(T::Val{:OID}, handle, col)          = getObjectId(handle)
gdbvalue(T::Val{:Geometry}, handle, col)     = getGeometryByIndex(handle, col-1)
gdbvalue(T::Val{:Blob}, handle, col)         = getBlobByIndex(handle, col-1)
gdbvalue(T::Val{:Raster}, handle, col)       = getRasterByIndex(handle, col-1)
gdbvalue(T::Val{:GUID}, handle, col)         = getGuidByIndex(handle, col-1)
gdbvalue(T::Val{:GlobalID}, handle, col)     = getGlobalIdByIndex(handle, col-1)
gdbvalue(T::Val{:XML}, handle, col)          = getXmlByIndex(handle, col-1)


mutable struct Database
    ref::Ptr{Cvoid}  # Reference to the internal data structure
    path::String
    opened::Bool

    function Database(f::String)
      if ! isempty(f)
        handle = _open_database(f)
        db = new(handle, f, true)
        finalizer(_close_database, db)
        return db
      else
        # TODO: also test file existence
        db = new(C_NULL, f, false)
        finalizer(_close_database, db)
        error("Empty database path.")
      end
    end
end


mutable struct Table
  ref::Ptr{Cvoid}
  name::String
  db::Database
  opened::Bool

  function Table(db::Database, f::String)
    if db.ref != C_NULL || db.opened
      if ! isempty(f)
        handle = _open_table(db, f)
        tbl = new(handle, f, db, true)
        finalizer(_close_table, tbl)
        return tbl
      else
        # TODO: also test file existence
        tbl = new(C_NULL, f, db, false)
        finalizer(_close_table, tbl)
        error("Empty table name.")
      end
    else
      error("Database not loaded.")
    end
  end
end


mutable struct FieldInfo
  ref::Ptr{Cvoid}

  function FieldInfo(ptr::Ptr{Cvoid})
    fdi = new(ptr)
    finalizer(_destroy_fieldinfo, fdi)
  end
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

  function QueryObj(ref::Ptr{Cvoid}, tbl::Table, db::Database)
    qobj = new(ref, tbl, db)
    finalizer(_close_query, qobj)
    return qobj
  end
end


mutable struct Row
  ref::Ptr{Cvoid}
  query::QueryObj
  tbl::Table
  db::Database

  function Row(ref, query, tbl, db)
    obj = new(ref, query, tbl, db)
    finalizer(_destroy_row, obj)
    return obj
  end
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
    0 => :SmallInteger,
    1 => :Integer,
    2 => :Single,
    3 => :Double,
    4 => :String,
    5 => :Date,
    6 => :OID,
    7 => :Geometry,
    8 => :Blob,
    9 => :Raster,
   10 => :GUID,
   11 => :GlobalID,
   12 => :XML,
)


TypeConversion = Dict(
    0 => Int16,
    1 => Int32,
    2 => Float32,
    3 => Float64,
    4 => String,
    5 => Integer,  # TODO: store date as integer or string or object?
    6 => Int32,    # TODO: OID
    7 => String,   # TODO: Geometry
    8 => String,   # TODO: Blob
    9 => String,   # TODO: Raster
   10 => String,    # TODO: GUID
   11 => String, # TODO: GlobalID
   12 => String,   # TODO: XML
)


close(obj::Database) = _close_database(obj)
close(obj::Table) = _close_table(obj)
close(obj::QueryObj) = _close_query(obj)
close(obj::FieldInfo) = _destroy_fieldinfo(obj)
close(obj::Row) = _destroy_row(obj)


describe(obj::Database) = _describe_db(obj)
describe(obj::Table) = _describe_table(obj)
describe(obj::QueryObj) = _describe_query(obj)
describe(obj::Row) = _describe_row(obj)
describe(obj::FieldInfo) = _describe_fieldinfo(obj)


function _open_database(path::String)
  ref = ccall((:gdb_create, libgeodb), Ptr{Cvoid}, ())
  if ref != C_NULL
    ret = ccall((:gdb_open, libgeodb), Int32, 
      (Ptr{Cvoid},Cwstring), ref, path)

    if ret != 0
      closeDatabase(ref)
      ref = C_NULL
    end
  end
  return ref
end


function _close_database(db::Database)
    if db.ref != C_NULL
        ccall((:gdb_close, libgeodb), Cvoid, (Ptr{Cvoid},), db.ref)
        db.ref = C_NULL
        db.opened = false
    end
end


function _describe_database(db::Database)
  # TODO: implement
  return
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


function _open_table(db::Database, table::String)
  ref = ccall((:gdbtable_create, libgeodb), Ptr{Cvoid}, ())
  if ref != C_NULL
    ret = ccall((:gdbtable_open, libgeodb), Int32, 
      (Ptr{Cvoid},Ptr{Cvoid},Cwstring), ref, db.ref, table)

    if ret != 0
      _close_table(ref)
      ref = C_NULL
    end
  end
  return ref
end


function _close_table(tbl::Table)
    if tbl.ref != C_NULL
        ccall((:gdbtable_close, libgeodb), Cvoid, 
          (Ptr{Cvoid},Ptr{Cvoid},), tbl.db.ref, tbl.ref)
        tbl.ref = C_NULL
        tbl.opened = false
    end
end


function _describe_table(table::Table)
  _describe_fieldinfo(getTableFieldInfo(table))
  return
end


function getTableRowsCount(tbl::Table)
  if tbl.ref != C_NULL
    return ccall((:gdbtable_get_row_count, libgeodb), Int32, 
      (Ptr{Cvoid},), tbl.ref)
  end
  return 0
end


function getTableFieldInfo(tbl::Table)
  fdi = FieldInfo(C_NULL)
  if tbl.ref != C_NULL
    ref = ccall((:gdbtable_get_fieldinfo, libgeodb), Ptr{Cvoid}, 
      (Ptr{Cvoid},), tbl.ref)
    fdi.ref = ref
  end
  return fdi
end


function getFieldInfoCount(fieldinfo::FieldInfo)
  if fieldinfo.ref != C_NULL
    return ccall((:gdbfieldinfo_get_count, libgeodb), Int32, 
                   (Ptr{Cvoid},), fieldinfo.ref,)
  end
  return 0
end

function getFieldInfoName(fieldinfo::FieldInfo, index::Int)
  if fieldinfo.ref != C_NULL
    name = Vector{Cwchar_t}(undef, 256)

    len = ccall((:gdbfieldinfo_get_name, libgeodb), Int32, 
                    (Ptr{Cvoid}, Int32, Ptr{Cwchar_t}, Int32), 
                    fieldinfo.ref, index, name, 255)

    return transcode(String, name[1:len])
  end
  return ""
end

function getFieldInfoType(fieldinfo::FieldInfo, index::Int)
  if fieldinfo.ref != C_NULL
    return ccall((:gdbfieldinfo_get_type, libgeodb), Int32, 
                   (Ptr{Cvoid}, Int32), fieldinfo.ref, index)
  end
  return 0
end

function getFieldInfoLength(fieldinfo::FieldInfo, index::Int)
  if fieldinfo.ref != C_NULL
    return ccall((:gdbfieldinfo_get_length, libgeodb), Int32, 
                   (Ptr{Cvoid}, Int32), fieldinfo.ref, index)
  end
  return 0
end

function getFieldInfoIsNullable(fieldinfo::FieldInfo, index::Int)
  if fieldinfo.ref != C_NULL
    return ccall((:gdbfieldinfo_get_is_nullable, libgeodb), Bool, 
                   (Ptr{Cvoid}, Int32), fieldinfo.ref, index)
  end
  return false
end

function _destroy_fieldinfo(fieldinfo::FieldInfo)
  if fieldinfo.ref != C_NULL
    ccall((:gdbfieldinfo_destroy, libgeodb), Int32, 
           (Ptr{Cvoid},), fieldinfo.ref,)
    fieldinfo.ref = C_NULL
  end
end


function _describe_fieldinfo(fieldinfo::FieldInfo)
  # TODO: implement
  fcount = Geodatabase.getFieldInfoCount(fieldinfo)
  println(" "*string(fcount)*" fields:")
  for field in 1:fcount
    name = Geodatabase.getFieldInfoName(fieldinfo, field-1)
    ftype = Geodatabase.TypeNames[Geodatabase.getFieldInfoType(fieldinfo, field-1)]
    println(" - "*name*" : "*ftype)
  end
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
    query = _open_query(tbl)
    if query.ref != C_NULL
      # TODO: transcode subfields and whereClause
      wsubfields = transcode(Cwchar_t, subfields)
      wwhereClause = transcode(Cwchar_t, whereClause)
      push!(wsubfields, 0)
      push!(wwhereClause, 0)
      ret = ccall((:gdbtable_search, libgeodb), Int32, 
                      (Ptr{Cvoid},Ptr{Cvoid},Ptr{Cwchar_t},Ptr{Cwchar_t}),
                      tbl.ref, query.ref, wsubfields, wwhereClause)
      if ret != 0
        _close_query(query)
      end
    end
    return query
  end
  return QueryObj(C_NULL, tbl, tbl.db)
end


function getTableFieldInfo(tbl::Table)
  fdi = FieldInfo(C_NULL)
  if tbl.ref != C_NULL
    ref = ccall((:gdbtable_get_fieldinfo, libgeodb), Ptr{Cvoid}, 
      (Ptr{Cvoid},), tbl.ref)
    fdi.ref = ref
  end
  return fdi
end

function getTableFieldsCount(table)
  return getFieldInfoCount(getTableFieldInfo(table))
end


function getTableFieldType(table, index)
  return getFieldInfoType(getTableFieldInfo(table), index)
end


function _open_query(tbl::Table)
  query = QueryObj(C_NULL, tbl, tbl.db)
  query.ref = ccall((:gdbquery_create, libgeodb), Ptr{Cvoid}, ())
  # TODO: check if the query is successful and set a variable
  return query
end


function _close_query(query::QueryObj)
  if query.ref != C_NULL
    ret = ccall((:gdbquery_close, libgeodb), Int32, (Ptr{Cvoid},), query.ref)
    query.ref = C_NULL
  end
end


function _describe_query(query::QueryObj)
  _describe_fieldinfo(getQueryFieldInfo(query))
end


function getQueryFieldsCount(query)
  return getFieldInfoCount(getQueryFieldInfo(query))
end


function getQueryFieldType(query, index)
  return getFieldInfoType(getQueryFieldInfo(query), index)
end


function getQueryFieldInfo(query::QueryObj)
  fdi = FieldInfo(C_NULL)
  if query.ref != C_NULL
    ref = ccall((:gdbquery_get_fieldinfo, libgeodb), Ptr{Cvoid}, 
      (Ptr{Cvoid},), query.ref)
    fdi.ref = ref
  end
  return fdi
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


function _destroy_row(row::Row)
  if row.ref != C_NULL
    ccall((:gdbrow_destroy, libgeodb), Int32, 
           (Ptr{Cvoid},), row.ref,)
    row.ref = C_NULL
  end
end


function _describe_row(row::Row)
  _describe_fieldinfo(getRowFieldInfo(row))
end


function getRowFieldInfo(row::Row)
  fdi = FieldInfo(C_NULL)
  if row.ref != C_NULL
    ref = ccall((:gdbrow_get_fieldinfo, libgeodb), Ptr{Cvoid}, 
      (Ptr{Cvoid},), row.ref)
    fdi.ref = ref
  end
  return fdi
end


function getRowFieldCount(row)
  return getFieldInfoCount(getRowFieldInfo(row))
end

function getRowFieldType(row, index)
  return getFieldInfoType(getRowFieldInfo(row), index)
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


function getObjectId(row)
  if row.ref != C_NULL
    return ccall((:gdbrow_get_objectid, libgeodb), Int32,
                 (Ptr{Cvoid},), row.ref,)
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
    # TODO: own=true causes lots of hard-to-find bugs. own=false, otoh, may
    # cause leaks
    newarr = unsafe_wrap(Vector{Cwchar_t}, retstr, len, own=false)
    retval = transcode(String, newarr)
    #println("DBG '"*retval*"' ("*string(len)*")")
    return retval
  end
  error("Null row reference.")
end


function getDateByIndex(row, index)
  # TODO: return type may not be adequated
  if row.ref != C_NULL
    return ccall((:gdbrow_get_date_by_index, libgeodb), Int32,
                 (Ptr{Cvoid},Int32), row.ref, index)
  end
  error("Null row reference.")
end


function getGeometryByIndex(row, index)
  # TODO: implement
  return ""
  if row.ref != C_NULL
    ptr = C_NULL
    return ccall((:gdbrow_get_geometry_by_index, libgeodb), Float64,
                 (Ptr{Cvoid},Int32,Ptr{Cvoid}), row.ref, index, ptr)
  end
  error("Null row reference.")
end


function getBlobByIndex(row, index)
  # TODO: implement
  return ""
end


function getRasterByIndex(row, index)
  # TODO: implement
  return ""
end


function getGuidByIndex(row, index)
  # TODO: implement
  return ""
end


function getGlobalIdByIndex(row, index)
  # TODO: implement
  return ""
end


function getXmlByIndex(row, index)
  # TODO: implement
  return ""
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