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

    println("Testing a query...")
    tbl = Geodatabase.openTable(db, "\\UNTRS")
    q = Geodatabase.searchTable(tbl, "SUB, BARR_1, BARR_2, MUN", "")
    if q.ref != C_NULL
      println("Search ok,")
      
      row = Geodatabase.nextQuery(q)
      fields = Geodatabase.getRowFields(row)
      fcount = length(fields)
      println("Returned with "*string(fcount)*" fields.")
      for field in fields
          println("     - "*field.name*" : "*field.type)
      end
      rcount = 0
      println("First row values:")
      for index = 0:fcount-1
        print(Geodatabase.getStringByIndex(row, index)*", ")
      end
      print("\n")
      while row.ref != C_NULL
        global row = Geodatabase.nextQuery(q)
        global rcount +=  1
      end
      Geodatabase.closeQuery(q)
      Geodatabase.closeTable(tbl)
      println("There are "*string(rcount)*" rows.")
    end
  else
    println("Error trying to connect to the database.")
  end
  
  Geodatabase.closeDatabase(db)
  println("Database closed.")

else
  println("Failed.")
end
