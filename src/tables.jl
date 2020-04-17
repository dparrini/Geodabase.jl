using Tables

sym(ptr) = ccall(:jl_symbol, Ref{Symbol}, (Ptr{UInt8},), ptr)

struct GdbQuery{NT}
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
  # TODO
  # sqlite3_reset(q.stmt.handle)
  # q.status[] = execute!(q.stmt)
  # return
end

function done(q::GdbQuery)
  return q.ref == C_NULL
end

function getvalue(q::GdbQuery, col::Int, ::Type{T}) where {T}
  isnull = getIsNullByIndex(q.row, col-1)
  if isnull
      return missing
  else
    TT = getRowFieldType(q.row, col-1)
    return gdbvalue(ifelse(TT === Any && !isbitstype(T), T, TT), q.row, col)
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
    query = searchTable(table::Table, subfields, whereClause)
    cols = getQueryFieldsCount(query)
    fields = getQueryFields(query)
    header = Vector{Symbol}(undef, cols)
    types = Vector{Type}(undef, cols)
    for i = 1:cols
        header[i] = Symbol(fields[i].name)
        if fields[i].nullable
            types[i] = stricttypes ? Union{fields[i].type, Missing} : Any
        else
            types[i] = stricttypes ? fields[i].type : Any
        end
    end

    # TODO: may skip the first row... check out
    row = nextQuery(query)
    return GdbQuery{NamedTuple{Tuple(header), Tuple{types...}}}(query, row)
  end
  return nothing
end
