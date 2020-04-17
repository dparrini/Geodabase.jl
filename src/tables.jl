using Tables

mutable struct GdbQuery{NT}
    ref::QueryObj
    row::Row
end

Tables.istable(::Type{<:GdbQuery}) = true
Tables.rowaccess(::Type{<:GdbQuery}) = true
Tables.rows(q::GdbQuery) = q
Tables.schema(q::GdbQuery{NamedTuple{names, types}}) where {names, types} = Tables.Schema(names, types)

Base.IteratorSize(::Type{<:GdbQuery}) = Base.SizeUnknown()
Base.eltype(q::GdbQuery{NT}) where {NT} = NT


function reset!(q::GdbQuery)
  error("reset! not implemented yet")
end

function done(q::GdbQuery)
  if q.row.ref == C_NULL
  end
  return q.row.ref == C_NULL
end

function getvalue(q::GdbQuery, col::Int, ::Type{T}) where {T}
  isnull = getIsNullByIndex(q.row, col-1)
  if isnull
      return missing
  else
    TT = TypeSymbols[getQueryFieldType(q.ref, col-1)]
    val = gdbvalue(ifelse(TT === Any && !isbitstype(T), T, TT), q.row, col)
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

function Base.iterate(q::GdbQuery{NT}) where {NT}
  done(q) && return nothing
  nt = generate_namedtuple(NT, q)
  return nt, nothing
end

function Base.iterate(q::GdbQuery{NT}, ::Nothing) where {NT}
  q.row = nextQuery(q.ref)
  done(q) && return nothing
  nt = generate_namedtuple(NT, q)
  return nt, nothing
end

"""
`SQLite.Query(db, sql::String; values=[]; stricttypes::Bool=true, nullable::Bool=true)`

Constructs a `SQLite.Query` object by executing the SQL query `sql` against the sqlite database `db` and querying
the columns names and types of the result set, if any.

Will bind `values` to any parameters in `sql`.
`stricttypes=false` will remove strict column typing in the result set, making each column effectively `Vector{Any}`; in sqlite, individual
column values are only loosely associated with declared column types, and instead each carry their own type information. This can lead to
type errors when trying to query columns when a single type is expected.
`nullable` controls whether `NULL` (`missing` in Julia) values are expected in a column.

An `SQLite.Query` object will iterate NamedTuple rows by default, and also supports the Tables.jl interface for integrating with
any other Tables.jl implementation. Due note however that iterating an sqlite result set is a forward-once-only operation. If you need
to iterate over an `SQLite.Query` multiple times, but can't store the iterated NamedTuples, call `SQLite.reset!(q::SQLite.Query)` to
re-execute the query and position the iterator back at the begining of the result set.
"""
function Query(table::Table, subfields::AbstractString, whereClause::AbstractString; stricttypes::Bool=true, nullable::Bool=true)
  if table.ref != C_NULL
    query = searchTable(table, subfields, whereClause)
    if query.ref != C_NULL
      # query fields is buggy, so its better to avoid its methods
      cols = getQueryFieldsCount(query)
      fdi = getQueryFieldInfo(query)
      header = Vector{Symbol}(undef, cols)
      types = Vector{Type}(undef, cols)
      for i = 1:cols
          header[i] = Symbol(getFieldInfoName(fdi, i-1))
          ftype = TypeSymbols[getFieldInfoType(fdi, i-1)]
          if getFieldInfoIsNullable(fdi, i-1)
              types[i] = stricttypes ? Union{ftype, Missing} : Any
          else
              types[i] = stricttypes ? ftype : Any
          end
      end
      row = nextQuery(query)
      return GdbQuery{NamedTuple{Tuple(header), Tuple{types...}}}(query, row)
    end
  end
  q = QueryObj(C_NULL, table, table.db)
  row = Row(C_NULL, q, table, table.db)
  header = Vector{Symbol}(undef, 0)
  types = Vector{Type}(undef, 0)
  return GdbQuery{NamedTuple{Tuple(header), Tuple{types...}}}(q, row)
end
