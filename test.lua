
package.cpath = "./lib/?.so;" .. package.cpath

local odbc = require("resty.odbc")

local conn = odbc:connect("PostgreSQL", "postgres", "")

local cursor = conn:execute("SELECT pk, name FROM foo;")

local row = {}
while cursor:fetch(row) do
  print(row[1])
  print(row[2])
end

