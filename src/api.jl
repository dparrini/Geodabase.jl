


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
    return
end


function getTablesCount(db::Database)
    if db.ref != C_NULL
        return  ccall((:gdb_get_tables_count, libgeodb), Int32, (Ptr{Cvoid},), db.ref)
    end
    return 0
end


function tablenames(db::Database)
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
      _close_table(ref, db.ref)
      ref = C_NULL
    end
  end
  return ref
end


function _close_table(tbl::Table)
  if tbl.ref != C_NULL
    _close_table(tbl.ref, tbl.db.ref)
    tbl.ref = C_NULL
    tbl.opened = false
  end
  return
end


function _close_table(tbl_ref::Ptr{Nothing}, db_ref::Ptr{Nothing})
    if tbl_ref != C_NULL && db_ref != C_NULL
        ccall((:gdbtable_close, libgeodb), Cvoid, 
          (Ptr{Cvoid},Ptr{Cvoid},), db_ref, tbl_ref)
    end
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
  return
end


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
  return Query(C_NULL, tbl, tbl.db)
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


function _open_query(tbl::Table)
  query = Query(C_NULL, tbl, tbl.db)
  query.ref = ccall((:gdbquery_create, libgeodb), Ptr{Cvoid}, ())
  # TODO: check if the query is successful and set a variable
  return query
end


function _close_query(query::Query)
  if query.ref != C_NULL
    ret = ccall((:gdbquery_close, libgeodb), Int32, (Ptr{Cvoid},), query.ref)
    query.ref = C_NULL
  end
  return
end


function getQueryFieldInfo(query::Query)
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


function nextQuery(query::Query)
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
  return
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


function getRowFieldInfo(row::Row)
  fdi = FieldInfo(C_NULL)
  if row.ref != C_NULL
    ref = ccall((:gdbrow_get_fieldinfo, libgeodb), Ptr{Cvoid}, 
      (Ptr{Cvoid},), row.ref)
    fdi.ref = ref
  end
  return fdi
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