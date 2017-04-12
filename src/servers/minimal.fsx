#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#load "../common/serializer.fs"
#else
module Services.Minimal
#endif
open System
open System.IO
open System.Collections.Generic
open Services.Serializer

// ----------------------------------------------------------------------------
// Server
// ----------------------------------------------------------------------------

#r "FSharp.Data.SqlClient/lib/net40/FSharp.Data.SqlClient.dll"
open FSharp.Data
open System.Data.SqlClient

#if INTERACTIVE
#load "../common/config.fs"
let connStr = Config.TheGammaSql
#else
let connStr = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_SQL")
#endif

// ------------------------------------------------------------------------------------------------

type Comparison = 
  | Equals
  | GreaterThan 
  | GreaterThanEqual
  | LessThan
  | LessThanEqual

type SqlExpression = 
  | Compare of string * Comparison * obj
  | And of SqlExpression * SqlExpression
  | Or of SqlExpression * SqlExpression
  | Empty

let rec format nextName expr = 
  match expr with 
  | Empty -> "(1=1)", []
  | Compare(col, comp, v) ->
      let op = 
        match comp with 
        | Equals -> "=" | LessThan -> "<" | GreaterThan -> ">"
        | LessThanEqual -> "<=" | GreaterThanEqual -> ">="
      let par = nextName ()
      sprintf "([%s] %s @%s)" col op par, [par, v]
  | And(e, Empty) | And(Empty, e) -> format nextName e
  | And(e1, e2) ->
      let s1, p1 = format nextName e1
      let s2, p2 = format nextName e2
      sprintf "(%s AND %s)" s1 s2, (p1 @ p2)
  | Or(e1, e2) ->
      let s1, p1 = format nextName e1
      let s2, p2 = format nextName e2
      sprintf "(%s OR %s)" s1 s2, (p1 @ p2)

let createFilteredCommand conn baseSql filter = 
  let pcount = ref 0
  let nextName () = incr pcount; sprintf "p%d" pcount.Value
  let cond, pars = format nextName filter
  let sql = baseSql (" WHERE " + cond)
  printfn "%s\n" sql
  let cmd = new SqlCommand(sql, conn, CommandTimeout=5*60)
  for pn, pv in pars do cmd.Parameters.AddWithValue(pn, pv) |> ignore
  cmd 


// ------------------------------------------------------------------------------------------------

let readKeyValuePair<'K> cmd = 
  use conn = new SqlConnection(connStr)
  conn.Open()
  use cmd = new SqlCommand(cmd, conn)
  use rdr = cmd.ExecuteReader()
  [| while rdr.Read() do yield unbox<'K>(rdr.GetValue(0)), rdr.GetValue(1).ToString() |]

let readDistinct column filter =
  use conn = new SqlConnection(connStr)
  conn.Open()
  let baseSql = sprintf "SELECT DISTINCT %s FROM [defra-airquality-monthly-measurement] %s" column
  use cmd = createFilteredCommand conn baseSql filter
  use rdr = cmd.ExecuteReader()
  [| while rdr.Read() do yield rdr.GetValue(0) |]
  
let readData daily filter =
  use conn = new SqlConnection(connStr)
  conn.Open()
  let baseSql = 
    if daily then sprintf "SELECT [Value],[Year],[Month],[Day] FROM [defra-airquality-daily-measurement] %s ORDER BY [Year] ASC, [Month] ASC, [Day] ASC" 
    else sprintf "SELECT [Value],[Year],[Month],1 AS [Day] FROM [defra-airquality-monthly-measurement] %s ORDER BY [Year] ASC, [Month] ASC" 
  use cmd = createFilteredCommand conn baseSql filter
  use rdr = cmd.ExecuteReader()
  [| while rdr.Read() do 
      let dt = DateTimeOffset(rdr.GetInt32(1), rdr.GetInt32(2), rdr.GetInt32(3), 0, 0, 0, TimeSpan.FromHours 0.)
      yield dt, rdr.GetDouble(0) |]

let pollutants = readKeyValuePair<int> "SELECT [ID],[Label] FROM [defra-airquality-pollutant]"
let stations = readKeyValuePair<string> "SELECT [ID],[Name] FROM [defra-airquality-station]"

(*
readDistinct "PollutantID" (Compare("StationID", Equals, box "ED"))
readData false (Compare("StationID", Equals, box "ED"))
readData false (And(Compare("StationID", Equals, box "ED"), Compare("Year", GreaterThan, box 1999)))
readData true (And(Compare("StationID", Equals, box "ED"), Compare("Year", Equals, box 2002)))
*)

// ------------------------------------------------------------------------------------------------

type Filter = SqlExpression

type Facet = 
  | Filter of Filter
  | Choice of (Filter -> list<string * string * Facet>)

let filterStations = function
  | Empty -> stations
  | cond ->
      let ids = readDistinct "StationID" cond |> Seq.map unbox<string> |> HashSet
      stations |> Array.filter (fst >> ids.Contains)

let filterPollutants = function
  | Empty -> pollutants |> Seq.map (fun (k,v) -> string k, v)
  | cond ->
      let ids = readDistinct "PollutantID" cond |> Seq.map unbox<int> |> HashSet
      pollutants |> Seq.filter (fst >> ids.Contains) |> Seq.map (fun (k,v) -> string k, v)

let filterYears filter = seq {
  match filter with 
  | Empty -> for y in 1973 .. 2017 -> y, string y 
  | cond -> for y in readDistinct "Year" cond |> Seq.map unbox<int> |> Seq.sort -> y, string y }

let months = 
  [ for i in 1 .. 12 -> 
    i, System.Globalization.DateTimeFormatInfo.InvariantInfo.GetMonthName(i) ]


let dateFacets = 
  let makeRangeCondition (y1,m1) (y2,m2) =
    if y1 = y2 then 
      And(
        Compare("Year", Equals, box y1),
        And(Compare("Month", GreaterThanEqual, box m1), Compare("Month", LessThanEqual, box m2)))
    else
      And(  
        Or(Compare("Year", GreaterThan, box y1),
          And(Compare("Year", Equals, box y1), Compare("Month", GreaterThanEqual, box m1))),
        Or(Compare("Year", LessThan, box y2),
          And(Compare("Year", Equals, box y2), Compare("Month", LessThanEqual, box m2))) )

  [ ( "year", "for a selected year",
      Choice(fun filters -> 
        [ for y, name in filterYears filters -> 
            string y, name, Filter(Compare("Year", Equals, box y)) ]))
    ( "month", "for a selected month",
      Choice(fun filters ->
        [ for y, name in filterYears filters -> 
            string y, name, Choice(fun filters ->            
              [ for m, name in months ->   
                  let cond = And(Compare("Year", Equals, box y), Compare("Month", LessThanEqual, box y))
                  string m, name, Filter(cond) ]) ]) )
    ( "custom", "for a custom range",
      Choice(fun filters ->
        [ for y1, name in filterYears filters -> 
            string y1, "from " + name, Choice(fun filters ->            
              [ for m1, name in months ->   
                  string m1, name, Choice(fun filters ->
                    [ for y2, name in filterYears filters -> 
                        string y2, "until " + name, Choice(fun filters ->            
                          [ for m2, name in months ->                               
                              string m2, name, Filter(makeRangeCondition (y1,m1) (y2,m2)) ]) ]) ]) ]) ) ]

let facets = 
  [ ( "date", "by date", Choice(fun _ -> dateFacets) )
    ( "station", "by station",
      Choice(fun filters -> 
        [ for id, name in filterStations filters -> 
            id, name, Filter(Compare("StationID", Equals, box id)) ]))
    ( "pollutant", "by pollutant",
      Choice(fun filters ->
        [ for id, name in filterPollutants filters -> 
            id, name, Filter(Compare("PollutantID", Equals, box (int id))) ])) ]


// ------------------------------------------------------------------------------------------------

type Type =
  | Named of string
  | Seq of Type
  | Tuple of Type * Type
  | Record of (string * Type) list

type Result = 
  | Primitive of typ:Type * endpoint:string
  | Nested of endpoint:string

type Member = 
  | Property of name:string * returns:Result * trace:seq<string>

let rec serializeType = function
  | Type.Named n -> JsonValue.String n
  | Type.Record(flds) ->
      [| "name", JsonValue.String "record"
         "fields", JsonValue.Array [| 
            for n, t in flds -> 
              JsonValue.Record [|
                "name", JsonValue.String n
                "type", serializeType t |] |] |]
      |> JsonValue.Record
  | Type.Tuple(t1, t2) ->
      [| "name", JsonValue.String "tuple"
         "params", JsonValue.Array [| serializeType t1; serializeType t2 |] |]
      |> JsonValue.Record
  | Type.Seq(t) -> 
      [| "name", JsonValue.String "seq"
         "params", JsonValue.Array [| serializeType t |] |]
      |> JsonValue.Record
  
let serializeResult = function
  | Result.Primitive(t, e) -> 
      [| "kind", JsonValue.String "primitive"
         "type", serializeType t
         "endpoint", JsonValue.String e |]
      |> JsonValue.Record
  | Result.Nested(e) -> 
      [| "kind", JsonValue.String "nested"
         "endpoint", JsonValue.String e |]
      |> JsonValue.Record

let serializeMember = function
  | Property(n, r, t) ->
      [| "name", JsonValue.String n
         "returns", serializeResult r
         "trace", JsonValue.Array [| for s in t -> JsonValue.String s |] |] 
      |> JsonValue.Record

let returnMembers members = 
  let json = [| for m in members -> serializeMember m |] |> JsonValue.Array
  json.ToString() |> Suave.Successful.OK

// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

let (</>) (a:string) (b:string) = a.TrimEnd('/') + "/" + b.TrimStart('/')

let rec findFacet filter facets pathHead pathTail =
  let facet = facets |> Seq.pick (fun (id, _, f) -> if pathHead = id then Some f else None)
  match pathTail, facet with
  | (_, Filter _ | [], _) -> facet
  | p::ps, Choice f -> findFacet filter (f filter) p ps

let (|Filtered|Filtering|) (url:string) =
  let filters = 
    url.Trim('/').Split([|"filter/"|], StringSplitOptions.RemoveEmptyEntries)
    |> Array.map (fun filter -> 
        let parts = filter.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> List.ofSeq
        List.head parts, List.tail parts )

  let filterOrFacet = filters |> Seq.fold (fun cond (firstPart, otherParts) ->
    let cond = match cond with Choice1Of2 cond -> cond | _ -> failwith "Unfinished condition"
    match findFacet cond facets firstPart otherParts with
    | Filter c -> Choice1Of2(And(c, cond))
    | Choice f -> Choice2Of2 (f cond)) (Choice1Of2 Empty)
  match filterOrFacet with
  | Choice1Of2 f -> Filtered(Seq.map fst filters, f)
  | Choice2Of2 f -> Filtering f

let getDataMembers trace applied = seq {
  let applied = set applied
  if applied.Contains("pollutant") then
    if applied.Contains("station") then
      yield Property("get monthly averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace |])
      yield Property("get yearly averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace |])
      yield Property("get daily averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace |])
    else
      yield Property("get station averages", Primitive(Seq(Tuple(Named "float", Named "float")), "/data"), [| trace |]) }

let app = 
  choose [
    path "/data" >=> request (fun r ->
      match Utils.ASCII.toString r.rawForm with
      | Filtering _ -> RequestErrors.BAD_REQUEST "Incomplete query specified"
      | Filtered(_, filter) ->
          let data = readData true filter
          let result = 
            [| for dt, v in data -> JsonValue.Array [| JsonValue.String (dt.ToString "o"); JsonValue.Float v |] |]
            |> JsonValue.Array
          Successful.OK (result.ToString()) ) 
    request (fun r -> 
      match r.url.LocalPath with
      | Filtered(applied, filter) ->
          returnMembers [ 
            let trace = r.url.LocalPath.Trim('/')
            yield! getDataMembers trace applied 
            for id, name, _ in facets do
              if applied |> Seq.exists ((=) id) |> not then 
                yield Property(name, Nested(r.url.LocalPath </> "filter" </> id), [| |]) ] 
      | Filtering(facets) ->
          returnMembers [
            for id, name, _ in facets ->
              Property(name, Nested(r.url.LocalPath </> id), [| |])
          ]
    ) ]