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

    # tblcount = Geodatabase.getTablesCount(db)
    # println("There is "*string(tblcount)*" tables.")

    # for name in Geodatabase.getTableNames(db)
    #   println(" - "*name)
    #   tbl = Geodatabase.openTable(db, name)
    #   if tbl.opened
    #     rows = Geodatabase.getTableRowsCount(tbl)
    #     println("     Opened! There are "*string(rows)*" rows.")
    #     fields = Geodatabase.getTableFields(tbl)

    #     for field in fields
    #       println("     - "*field.name*" : "*field.typestr)
    #     end

    #     Geodatabase.closeTable(tbl)
    #   else
    #     println("     Coult not open it.")
    #   end
    # end

    println("Testing a query...")
    tbl = Geodatabase.Table(db, "\\UNTRS")
    if tbl.opened
      # q = Geodatabase.searchTable(tbl, "SUB, BARR_1, BARR_2, MUN", "")
      # if q.ref != C_NULL
      #   println("Search ok,")
        
      #   row = Geodatabase.nextQuery(q)
      #   fields = Geodatabase.getRowFields(row)
      #   fcount = length(fields)
      #   println("Returned with "*string(fcount)*" fields.")
      #   for field in fields
      #       println("     - "*field.name*" : "*field.typestr)
      #   end
      #   rcount = 0
      #   println("First row values:")
      #   for index = 0:fcount-1
      #     print(Geodatabase.getStringByIndex(row, index)*", ")
      #   end
      #   print("\n")
      #   while row.ref != C_NULL
      #     global row = Geodatabase.nextQuery(q)
      #     global rcount +=  1
      #   end
      #   Geodatabase.closeQuery(q)
      #   println("There are "*string(rcount)*" rows.")
      # end

      println("Testing another query (with Tables)...")

      q = Geodatabase.Query(tbl, "SUB, BARR_1, BARR_2, MUN", "")
      if q.row.ref != C_NULL
        println("Query before DF: "*string(q))


          CSV.write("testwrite.csv", q)
          # df = DataFrames.DataFrame(q)
          # print(df)
        try
          

          println("End?")
        catch e
          println("Something went wrong:")
          println(e)
        end
        Geodatabase.closeQuery(q.ref)
      end
      Geodatabase.closeTable(tbl)
    else
      println("Error trying to open table.")
    end
  else
    println("Error trying to connect to the database.")
  end

else
  println("Failed.")
end
