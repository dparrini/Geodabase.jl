module Geodatabase

export Database,
       Table,
       FieldInfo,
       Field,
       Query,
       Row,
       reset!,
       close,
       describe,
       columnnames,
       tablenames,
       Search


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
         ENV["DYLD_LIBRARY_PATH"] = string(get(ENV, "DYLD_LIBRARY_PATH", ""), ":", julia_libdir)
    elseif Sys.islinux()
         ENV["LD_LIBRARY_PATH"] = string(get(ENV, "LD_LIBRARY_PATH", ""), ":", julia_libdir, ":", libgeodb_libdir)
    end
end


"""
Handles a database connection.
"""
mutable struct Database
    ref::Ptr{Cvoid}  # Reference to the internal data structure
    path::String
    opened::Bool

    """
    Creates a database connection to the path specified in `file_path`.
    """
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

"""
Handles a Geodatabase table.
"""
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


"""
Handles a Geodatabase FieldInfo object, which describes fields of
a table, of a query result or of a row.
"""
mutable struct FieldInfo
  ref::Ptr{Cvoid}

  function FieldInfo(ptr::Ptr{Cvoid})
    fdi = new(ptr)
    finalizer(_destroy_fieldinfo, fdi)
  end
end


"""
Handles a Geodatabase Field object, which describes a field of a 
Geodatabase table, query result or of a row.
"""
mutable struct Field
  ref::Ptr{Cvoid}
  name::String
  alias::String
  type
  typestr::String
  db::Database
  len::Int32
  nullable::Bool
  # TODO: add a parent member (can point to Table, Query, Row)
end


"""
Handles a Geodatabase Query object.
"""
mutable struct Query
  ref::Ptr{Cvoid}
  tbl::Table
  db::Database

  function Query(ref::Ptr{Cvoid}, tbl::Table, db::Database)
    qobj = new(ref, tbl, db)
    finalizer(_close_query, qobj)
    return qobj
  end
end


"""
Handles a Geodatabase Row.
"""
mutable struct Row
  ref::Ptr{Cvoid}
  query::Query
  tbl::Table
  db::Database

  function Row(ref, query, tbl, db)
    obj = new(ref, query, tbl, db)
    finalizer(_destroy_row, obj)
    return obj
  end
end


"""
Frees Geodatabase resouces associated with the `obj`. 
It is called automatically when an `Geodatabase.jl` object (such as 
`Database`, `Table`, etc) is not used anymore.
"""
close(obj::Database) = _close_database(obj)
close(obj::Table) = _close_table(obj)
close(obj::Query) = _close_query(obj)
close(obj::FieldInfo) = _destroy_fieldinfo(obj)
close(obj::Row) = _destroy_row(obj)


"""
Prints schema information (columns and types) of `obj`, which can be of 
the types `Database`, `Table`, `Query`, and `Row`.
"""
describe(obj::Database) = _describe_db(obj)
describe(obj::Table) = _describe_table(obj)
describe(obj::Query) = _describe_query(obj)
describe(obj::Row) = _describe_row(obj)
describe(obj::FieldInfo) = _describe_fieldinfo(obj)


"""
Returns a `String` list of the column names of `obj`, which can be of the types 
`Table`, `Query`, and `Row`.
"""
columnnames(obj::Table) = _columnnames_table
columnnames(obj::Query) = _columnnames_query
columnnames(obj::Row) = _columnnames_row
columnnames(obj::FieldInfo) = _columnnames_fieldinfo


"""
Restores the results of a `Geodatabase.Search` call. 
"""
function reset!(q)
  newq = Search(q.ref.tbl, q.fields, q.where)
  q.ref = newq.ref
  q.row = newq.row
  return q
end



function _describe_database(db::Database)
  # TODO: implement
  return
end


function _describe_table(table::Table)
  _describe_fieldinfo(getTableFieldInfo(table))
  return
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
  return
end


function getTableFieldsCount(table)
  return getFieldInfoCount(getTableFieldInfo(table))
end


function getTableFieldType(table, index)
  return getFieldInfoType(getTableFieldInfo(table), index)
end


function _describe_query(query::Query)
  _describe_fieldinfo(getQueryFieldInfo(query))
end


function getQueryFieldsCount(query)
  return getFieldInfoCount(getQueryFieldInfo(query))
end


function getQueryFieldType(query, index)
  return getFieldInfoType(getQueryFieldInfo(query), index)
end


function _describe_row(row::Row)
  _describe_fieldinfo(getRowFieldInfo(row))
end


function getRowFieldCount(row)
  return getFieldInfoCount(getRowFieldInfo(row))
end

function getRowFieldType(row, index)
  return getFieldInfoType(getRowFieldInfo(row), index)
end


function _columnnames_table(obj)
  return _columnnames_fieldinfo(getTableFieldInfo(obj))
end


function _columnnames_Query(obj)
  return _columnnames_fieldinfo(getQueryFieldInfo(obj))
end


function _columnnames_row(obj)
  return _columnnames_fieldinfo(getRowFieldInfo(obj))
end


function _columnnames_fieldinfo(obj)
  fcount = getFieldInfoCount(obj)
  colnames = Vector{String}(undef, 0)
  for ifield = 1:fcount
    colnames[ifield] = getFieldInfoName(obj, ifield)
  end
  return colnames
end


include("api.jl")
include("tables.jl")


end # module