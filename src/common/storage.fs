namespace GovUk

open System
open System.IO
open System.Collections.Generic
open System.Data
open System.Data.SqlClient

// ------------------------------------------------------------------------------------------------
// Server
// ------------------------------------------------------------------------------------------------

type Comparison = 
  | Equals
  | GreaterThan 
  | GreaterThanEqual
  | LessThan
  | LessThanEqual

type SqlExpression = 
  | Column of string
  | UncheckedExpression of string
  | Compare of SqlExpression * Comparison * obj
  | And of SqlExpression * SqlExpression
  | Or of SqlExpression * SqlExpression
  | Empty

module Storage = 
  let rec private format nextName expr = 
    match expr with 
    | Column col -> sprintf "[%s]" col, []
    | UncheckedExpression e -> e, []
    | Empty -> "(1=1)", []
    | Compare(col, comp, v) ->
        let op = 
          match comp with 
          | Equals -> "=" | LessThan -> "<" | GreaterThan -> ">"
          | LessThanEqual -> "<=" | GreaterThanEqual -> ">="
        let par = nextName ()
        let left, pars = format nextName col
        sprintf "(%s %s @%s)" left op par, (par, v)::pars
    | And(e, Empty) | And(Empty, e) -> format nextName e
    | And(e1, e2) ->
        let s1, p1 = format nextName e1
        let s2, p2 = format nextName e2
        sprintf "(%s AND %s)" s1 s2, (p1 @ p2)
    | Or(e1, e2) ->
        let s1, p1 = format nextName e1
        let s2, p2 = format nextName e2
        sprintf "(%s OR %s)" s1 s2, (p1 @ p2)

  let private createFilteredCommand conn baseSql filter = 
    let pcount = ref 0
    let nextName () = incr pcount; sprintf "p%d" pcount.Value
    let cond, pars = format nextName filter
    let sql = baseSql (" WHERE " + cond)
    printfn "%s\n" sql
    let cmd = new SqlCommand(sql, conn, CommandTimeout=5*60)
    for pn, pv in pars do cmd.Parameters.AddWithValue(pn, pv) |> ignore
    cmd 

  let readKeyValuePair connStr cmd = 
    use conn = new SqlConnection(connStr)
    conn.Open()
    use cmd = new SqlCommand(cmd, conn)
    use rdr = cmd.ExecuteReader()
    [| while rdr.Read() do yield rdr.GetValue(0), rdr.GetValue(1).ToString() |]

  let readDistinct connStr column table filter =
    use conn = new SqlConnection(connStr)
    conn.Open()
    let baseSql = sprintf "SELECT DISTINCT %s FROM [%s] %s" column table
    use cmd = createFilteredCommand conn baseSql filter
    use rdr = cmd.ExecuteReader()
    [| while rdr.Read() do yield rdr.GetValue(0) |]
  
  let readTimeSeriesData connStr columns table filter =
    use conn = new SqlConnection(connStr)
    conn.Open()
    let baseSql = sprintf "SELECT %s FROM %s %s ORDER BY [Year] ASC, [Month] ASC, [Day] ASC" columns table
    use cmd = createFilteredCommand conn baseSql filter
    use rdr = cmd.ExecuteReader()
    [| while rdr.Read() do 
        let dt = DateTimeOffset(rdr.GetInt32(1), rdr.GetInt32(2), rdr.GetInt32(3), 0, 0, 0, TimeSpan.FromHours 0.)
        yield dt, rdr.GetDouble(0) |]

  let readFacetedTimeSeriesData connStr facet columns table filter =
    use conn = new SqlConnection(connStr)
    conn.Open()
    let baseSql = sprintf "SELECT %s,%s FROM %s %s ORDER BY [Year] ASC, [Month] ASC, [Day] ASC" facet columns table
    use cmd = createFilteredCommand conn baseSql filter
    use rdr = cmd.ExecuteReader()
    [| while rdr.Read() do 
        let dt = DateTimeOffset(rdr.GetInt32(2), rdr.GetInt32(3), rdr.GetInt32(4), 0, 0, 0, TimeSpan.FromHours 0.)
        yield rdr.GetInt32(0), dt, rdr.GetDouble(1) |]
