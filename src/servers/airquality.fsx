#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#load "../common/storage.fs" "../common/serializer.fs" "../common/facets.fs"
#else
module GovUk.Airquality
#endif
open GovUk
open System
open System.IO
open System.Collections.Generic

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

#if INTERACTIVE
#load "../common/config.fs"
let connStr = Config.TheGammaSql
#else
let connStr = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_SQL")
#endif

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

let filterStations = 
  let stations = Storage.readKeyValuePair connStr "SELECT [ID],[Name] FROM [defra-airquality-station]"
  function
  | Empty -> stations
  | cond ->
      let ids = Storage.readDistinct connStr "[StationID]" "defra-airquality-monthly-measurement" cond |> HashSet
      stations |> Array.filter (fst >> ids.Contains)

let filterPollutants = 
  let pollutants = Storage.readKeyValuePair connStr "SELECT [ID],[Label] FROM [defra-airquality-pollutant]"
  function
  | Empty -> pollutants
  | cond ->
      let ids = Storage.readDistinct connStr "[PollutantID]" "defra-airquality-monthly-measurement" cond |> HashSet
      pollutants |> Array.filter (fst >> ids.Contains)

let filterYears = 
  let years = [| for y in 1973 .. 2017 -> box y, string y |]
  function
  | Empty -> years
  | cond ->
      let ids = Storage.readDistinct connStr "[Year]" "defra-airquality-monthly-measurement" cond |> HashSet
      years |> Array.filter (fst >> ids.Contains)

let months = 
  [ for i in 1 .. 12 -> 
    box i, System.Globalization.DateTimeFormatInfo.InvariantInfo.GetMonthName(i) ]

let dateFacets = 
  let makeRangeCondition (y1,m1) (y2,m2) =
    if y1 = y2 then 
      And(
        Compare(Column "Year", Equals, y1),
        And(Compare(Column "Month", GreaterThanEqual, m1), Compare(Column "Month", LessThanEqual, m2)))
    else
      And(  
        Or(Compare(Column "Year", GreaterThan, y1),
          And(Compare(Column "Year", Equals, y1), Compare(Column "Month", GreaterThanEqual, m1))),
        Or(Compare(Column "Year", LessThan, y2),
          And(Compare(Column "Year", Equals, y2), Compare(Column "Month", LessThanEqual, m2))) )

  [ ( box "year", "for a selected year",
      Choice(fun filters -> 
        [ for y, name in filterYears filters -> 
            y, name, Filter(Compare(Column "Year", Equals, y)) ]))
    ( box "month", "for a selected month",
      Choice(fun filters ->
        [ for y, name in filterYears filters -> 
            y, name, Choice(fun filters ->            
              [ for m, name in months ->   
                  let cond = And(Compare(Column "Year", Equals, y), Compare(Column "Month", LessThanEqual, m))
                  m, name, Filter(cond) ]) ]) )
    ( box "custom", "for a custom range",
      Choice(fun filters ->
        [ for y1, name in filterYears filters -> 
            y1, "from " + name, Choice(fun filters ->            
              [ for m1, name in months ->   
                  m1, name, Choice(fun filters ->
                    [ for y2, name in filterYears filters -> 
                        y2, "until " + name, Choice(fun filters ->            
                          [ for m2, name in months ->                               
                              m2, name, Filter(makeRangeCondition (y1,m1) (y2,m2)) ]) ]) ]) ]) ) ]

let facets = 
  [ ( box "date", "by date", Choice(fun _ -> dateFacets) )
    ( box "station", "by station",
      Choice(fun filters -> 
        [ for id, name in filterStations filters -> 
            id, name, Filter(Compare(Column "StationID", Equals, id)) ]))
    ( box "pollutant", "by pollutant",
      Choice(fun filters ->
        [ for id, name in filterPollutants filters -> 
            id, name, Filter(Compare(Column "PollutantID", Equals, id)) ])) ]


// ------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators
open GovUk

let getDataMembers trace applied = seq {
  let applied = set applied
  if applied.Contains("pollutant") then
    if applied.Contains("station") then
      yield Property("get monthly averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "monthly" |])
      yield Property("get yearly averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "yearly" |])
      yield Property("get daily averages", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "daily" |])
    else
      yield Property("get station averages", Primitive(Seq(Tuple(Named "float", Named "float")), "/data"), [| trace |]) }

open FSharp.Data
open Facets

let app = 
  Facets.createFacetedApp facets getDataMembers (fun kind filter ->  
    let data = 
      match kind with 
      | ["daily"] -> Storage.readTimeSeriesData connStr "[Value],[Year],[Month],[Day]" "[defra-airquality-daily-measurement]" filter
      | ["monthly"] -> Storage.readTimeSeriesData connStr "[Value],[Year],[Month],1 AS [Day]" "[defra-airquality-monthly-measurement]" filter
      | ["yearly"] -> 
          Storage.readTimeSeriesData connStr "[Value],[Year],[Month],1 AS [Day]" "[defra-airquality-monthly-measurement]" filter
          |> Array.groupBy (fun (dt, v) -> dt.Year)
          |> Array.map (fun (y, g) -> DateTimeOffset(y, 1, 1, 0, 0, 0, TimeSpan.FromHours 0.), Seq.averageBy snd g)
      | _ -> failwith "Unexpected data kind"
    [| for dt, v in data -> JsonValue.Array [| JsonValue.String (dt.ToString "o"); JsonValue.Float v |] |]
    |> JsonValue.Array
  )