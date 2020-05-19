using Tables


mutable struct GdbSearch{NT}
    ref::Query
    row::Row
    fields::String
    where::String
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


Tables.istable(::Type{<:GdbSearch}) = true
Tables.rowaccess(::Type{<:GdbSearch}) = true
Tables.rows(q::GdbSearch) = q
Tables.schema(q::GdbSearch{NamedTuple{names, types}}) where {names, types} = Tables.Schema(names, types)

Base.IteratorSize(::Type{<:GdbSearch}) = Base.SizeUnknown()
Base.eltype(q::GdbSearch{NT}) where {NT} = NT


function done(q::GdbSearch)
  if q.row.ref == C_NULL
  end
  return q.row.ref == C_NULL
end


function getvalue(q::GdbSearch, col::Int, ::Type{T}) where {T}
  isnull = getIsNullByIndex(q.row, col-1)
  if isnull
      return missing
  else
    type = getQueryFieldType(q.ref, col-1)
    TT = TypeSymbols[type]
    val = gdbvalue(Val{TT}(), q.row, col)
    return val
  end
end


function generate_namedtuple(::Type{NamedTuple{names, types}}, q) where {names, types}
  if @generated
    vals = Tuple(:(getvalue(q, $i, $(fieldtype(types, i)))) for i = 1:fieldcount(types))
    return :(NamedTuple{names, types}(($(vals...),)))
  else
    return NamedTuple{names, types}(Tuple(getvalue(q, i, fieldtype(types, i)) for i = 1:fieldcount(types)))
  end
end


function Base.iterate(q::GdbSearch{NT}) where {NT}
  done(q) && return nothing
  nt = generate_namedtuple(NT, q)
  return nt, nothing
end


function Base.iterate(q::GdbSearch{NT}, ::Nothing) where {NT}
  q.row = nextQuery(q.ref)
  done(q) && return nothing
  nt = generate_namedtuple(NT, q)
  return nt, nothing
end


"""
Constructs a `Geodatabase.GdbSearch` object by executing a search for `subfields`
given a `whereClause` condition against `table` of a `db` object.
"""
function Search(db::Database, table::AbstractString, subfields::AbstractString, 
  whereClause::AbstractString=""; stricttypes::Bool=true, nullable::Bool=true)
  tbl = Geodatabase.Table(db, table)
  if tbl.ref != C_NULL
    return Search(tbl, subfields, whereClause, stricttypes, nullable)
  end
  error("Error trying to open table \""*table*"\".")
end


"""
Constructs a `Geodatabase.GdbSearch` object by executing a search for `subfields`
given a `whereClause` condition against `table` object.
"""
function Search(table::Table, subfields::AbstractString, 
  whereClause::AbstractString=""; stricttypes::Bool=true, nullable::Bool=true)
  if table.ref != C_NULL
    query = searchTable(table, subfields, whereClause)
    if query.ref != C_NULL
      # query fields is buggy, so its better to avoid its methods
      cols = getQueryFieldsCount(query)
      fdi = getQueryFieldInfo(query)
      header = Vector{Symbol}(undef, cols)
      jltypes = Vector{Type}(undef, cols)
      dbtypes = Vector{Symbol}(undef, cols)
      for i = 1:cols
          header[i] = Symbol(getFieldInfoName(fdi, i-1))
          jtype = TypeConversion[getFieldInfoType(fdi, i-1)]
          dbtypes[i] = TypeSymbols[getFieldInfoType(fdi, i-1)]

          if getFieldInfoIsNullable(fdi, i-1)
              jltypes[i] = stricttypes ? Union{jtype, Missing} : Any
          else
              jltypes[i] = stricttypes ? jtype : Any
          end
      end
      row = nextQuery(query)
      return GdbSearch{NamedTuple{Tuple(header), Tuple{jltypes...}}}(query, row, subfields, whereClause)
    end
  end
  q = Query(C_NULL, table, table.db)
  row = Row(C_NULL, q, table, table.db)
  header = Vector{Symbol}(undef, 0)
  types = Vector{Type}(undef, 0)
  return GdbSearch{NamedTuple{Tuple(header), Tuple{types...}}}(q, row, subfields, whereClause)
end
