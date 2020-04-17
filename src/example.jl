using DataFrames
using CSV

include("Geodatabase.jl")
using .Geodatabase


file = "D:\\BDGD\\BANDEIRANTE_391_2017-12-31_M10_20180807-1511.gdb"

println("Trying to open "*file*"...")
db = Geodatabase.Database(file)


if db.ref != C_NULL
  if db.opened
    println("Opened successfuly. The ref is: "* string(db.ref))

    tblcount = Geodatabase.getTablesCount(db)
    println("There is "*string(tblcount)*" tables.")

    for i in 1:6
      for name in Geodatabase.getTableNames(db)
        println(" - "*name)
        tbl = Geodatabase.Table(db, name)
        if tbl.opened
          rows = Geodatabase.getTableRowsCount(tbl)
          println("     Opened! There are "*string(rows)*" rows.")
          fdi = Geodatabase.getTableFieldInfo(tbl)
          fcount = Geodatabase.getFieldInfoCount(fdi)

          for field in 1:fcount
            name = Geodatabase.getFieldInfoName(fdi, field-1)
            ftype = Geodatabase.TypeNames[Geodatabase.getFieldInfoType(fdi, field-1)]
            println("     - "*name*" : "*ftype)
          end
        else
          println("     Coult not open it.")
        end
      end
    end

    println("Testing a query...")
    tbl = Geodatabase.Table(db, "\\UNTRS")
    if tbl.opened

      # for i = 1:3
      #   q = Geodatabase.searchTable(tbl, "SUB, BARR_1, BARR_2, MUN", "")
      #   if q.ref != C_NULL
      #     println("Search ok ("*string(q.ref)*"),")
          
      #     row = Geodatabase.nextQuery(q)
      #     fdi = Geodatabase.getRowFieldInfo(row)
      #     fcount = Geodatabase.getRowFieldCount(row)
      #     println("Returned with "*string(fcount)*" fields.")
      #     for field in 1:fcount
      #       name = Geodatabase.getFieldInfoName(fdi, field-1)
      #       ftype = Geodatabase.TypeNames[Geodatabase.getFieldInfoType(fdi, field-1)]
      #       println("     - "*name*" : "*ftype)
      #     end
      #     rcount = 0
      #     println("First row values:")
      #     for index = 0:fcount-1
      #       print(Geodatabase.getStringByIndex(row, index)*", ")
      #     end
      #     print("\n")
      #     while row.ref != C_NULL
      #       row = Geodatabase.nextQuery(q)
      #       rcount +=  1
      #     end
      #     println("There are "*string(rcount)*" rows.")
      #   else
      #     println("Search failed.")
      #   end
      # end

      println("Testing another query (with Tables)...")

      abc = Geodatabase.Query(tbl, "SUB, BARR_1, BARR_2, MUN", "")
      if abc.row.ref != C_NULL
        println("Query before DF: "*string(abc))


          CSV.write("testwrite.csv", abc)
          # df = DataFrames.DataFrame(abc)
          # print(df)
          println("Ok, done.")
      end
    else
      println("Error trying to open table.")
    end
  else
    println("Error trying to connect to the database.")
  end

else
  println("Failed.")
end
