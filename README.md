# Geodatabase.jl

Implements an interface to the ESRI .gdb geodatabase files.


## Usage


### `Database(file_path::String)`

Creates a database connection to the path specified in `file_path`.

### `Table(db::Database, table_name::String)`

Opens a table `file_name` from `db` `Database` for queries.


### `Search(table::Table, fields::String, where_clause::String)`

Returns a query for `fields` of the `table` meeting the `where_clause`
criteria.

- `fields = "*"` to query all table columns.
- `fields = "name1, name2, ..., name_n"` to query a specific list of columns, in this specific order.
- `where_clause = ""` to query all rows.
- use common SQL statement operators to specify filters such as `=`, `>`, `AND`, `OR`, etc.

The returned value can be consumed only once (forward direction), for instance to produce a `DataFrame` or to write a CSV file. To reuse it, a `reset!` call must be made over the `Search` returned value.


### `describe(obj)`

Prints schema information (columns and types) of `obj`, which can be of the types `Database`, `Table`, `Query`, and `Row`.

### `close(obj)`

Frees `obj` resources. It is called automatically when an `Geodatabase.jl` object (such as `Database`, `Table`, etc) is not used anymore.

### `tablenames(db::Database)`

Returns a `String` list of the table names of `db`.

### `columnnames(obj)`

Returns a `String` list of the column names of `obj`, which can be of the types `Table`, `Query`, and `Row`.

### `reset!(obj::GdbSearch)`

Restores the results of a `Geodatabase.Search` call. 


## Example

```julia
include("Geodatabase.jl")
using .Geodatabase

# Load a .gdb database
db = Geodatabase.Database("the_database.gdb")

# List available tables
println(Geodatabase.tablenames(db))

table = Geodatabase.Table(db, "\\a_table")

# Query all available data from a table
CSV.Write("file.csv", Geodatabase.Search(table, "*", ""))

# Query some of the fields of a table and filter by one of the fields
DataFrames.DataFrame(Geodatabase.Search(table, "ObjectID, name, quantity", "quantity > 100"))

```


### Printing available table schema

```julia
db = Geodatabase.Database("a_database.gdb")
table_names = Geodatabase.tablenames(db)

for table_name in table_names
  table = Geodatabase.Table(db, table_name)
  Geodatabase.describe(table)
end
```


### Using the same query twice with `reset!`

```julia
query = Geodatabase.Search(table, "*", "")
CSV.Write("file.csv", query)

Geodatabase.reset!(query)
println(DataFrames.DataFrame(query))
```