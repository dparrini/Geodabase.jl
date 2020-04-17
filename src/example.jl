using DataFrames
using CSV

include("Geodatabase.jl")
using .Geodatabase


file = "D:\\BDGD\\BANDEIRANTE_391_2017-12-31_M10_20180807-1511.gdb"


function test_tables(db::Geodatabase.Database)
  for i in 1:1
    for name in Geodatabase.getTableNames(db)
      println(" - "*name)
      tbl = Geodatabase.Table(db, name)
      if tbl.opened
        rows = Geodatabase.getTableRowsCount(tbl)
        println("     Opened! There are "*string(rows)*" rows.")
        Geodatabase.describe(tbl)
      else
        println("     Coult not open it.")
      end
      Geodatabase.close(tbl)
    end
  end
end


function test_raw_query(db)
  println("Testing a query...")
  tbl = Geodatabase.Table(db, "\\UNTRS")
  if tbl.opened
    for i = 1:1
      q = Geodatabase.searchTable(tbl, "SUB, BARR_1, BARR_2, MUN", "")
      if q.ref != C_NULL
        println("Search OK.")
        row = Geodatabase.nextQuery(q)
        # Print query information
        Geodatabase.describe(row)
        # Number of columns
        cols = Geodatabase.getRowFieldCount(row)
        # Count number of rows
        rcount = 0
        println("First row values:")
        for index = 0:cols-1
          print(Geodatabase.getStringByIndex(row, index)*", ")
        end
        print("\n")
        while row.ref != C_NULL
          row = Geodatabase.nextQuery(q)
          rcount +=  1
        end
        println("There are "*string(rcount)*" rows.")
      else
        println("Search failed.")
      end
    end
  end
end


function test_Tablesjl(db::Geodatabase.Database)
  println("Testing query (with Tables, DataFrame, and CSV)...")
  tbl = Geodatabase.Table(db, "\\UNTRS")

  println("Testing Query -> CSV...")
  query1 = Geodatabase.Query(tbl, "SUB, BARR_1, BARR_2, MUN", "")
  if query1.row.ref != C_NULL
    CSV.write("testwrite.csv", query1)
    println(" Done.")
  else
    println("Query -> CSV test failed")
  end

  println("Testing Query -> DataFrame....")
  query2 = Geodatabase.Query(tbl, "SUB, BARR_1, BARR_2", "")
  if query2.row.ref != C_NULL
    df = DataFrames.DataFrame(query2)
    println(df)
    println()
    println("Ok, done.")
  else
    println("Query -> DataFrame test failed")
  end
end


function test_all(file::String)
  println("Trying to open "*file*"...")
  db = Geodatabase.Database(file)


  if db.ref != C_NULL
    if db.opened
      println("Opened successfuly. The ref is: "* string(db.ref))

      tblcount = Geodatabase.getTablesCount(db)
      println("There is "*string(tblcount)*" tables.")

      test_tables(db)

      test_raw_query(db)

      test_Tablesjl(db)

    else
      println("Error trying to connect to the database.")
    end

  else
    println("Failed.")
  end
end



test_all(file)

