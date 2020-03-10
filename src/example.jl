include("Geodatabase.jl")
using .Geodatabase

file = "D:\\BDGD\\BANDEIRANTE_391_2017-12-31_M10_20180807-1511.gdb"

println("Trying to open "*file*"...")
db = Geodatabase.openDatabase(file)


if db.ref != C_NULL
  if db.opened
    println("Opened successfuly. The ref is: "* string(db.ref))

    tblcount = Geodatabase.getTablesCount(db)
    println("There is "*string(tblcount)*" tables.")

    for name in Geodatabase.getTableNames(db)
      println(" - "*name)
      tbl = Geodatabase.openTable(db, name)
      if tbl.opened
        rows = Geodatabase.getTableRowsCount(tbl)
        println("     Opened! There are "*string(rows)*" rows.")
        fields = Geodatabase.getTableFields(tbl)

        for field in fields
          println("     - "*field.name*" : "*field.type)
        end

        Geodatabase.closeTable(tbl)
      else
        println("     Coult not open it.")
      end
    end
  else
    println("Error trying to connect to the database.")
  end
  
  Geodatabase.closeDatabase(db)
  println("Database closed.")

else
  println("Failed.")
end
