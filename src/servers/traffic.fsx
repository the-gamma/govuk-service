#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#r "Deedle/lib/net40/Deedle.dll"
#load "../common/storage.fs" "../common/serializer.fs" "../common/facets.fs"
#else
module GovUk.Traffic
#endif
open GovUk
open Deedle
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

let filterRegions = 
  let regions = Storage.readKeyValuePair connStr "SELECT [ID],[Region] FROM [gb-road-traffic-counts-region]"
  function
  | Empty -> regions
  | cond ->
      let ids = Storage.readDistinct connStr "[RegionID]" "gb-road-traffic-counts-daily-measurement" cond |> HashSet
      regions |> Array.filter (fst >> ids.Contains)

let filterLocations = 
  let locations = Storage.readKeyValuePair connStr "SELECT [ID],[Location] FROM [gb-road-traffic-counts-location]"
  function
  | Empty -> locations
  | cond ->
      let ids = Storage.readDistinct connStr "[LocationID]" "gb-road-traffic-counts-daily-measurement" cond |> HashSet
      locations |> Array.filter (fst >> ids.Contains)

let filterRoads = 
  let roads = Storage.readKeyValuePair connStr "SELECT [ID],[Road] FROM [gb-road-traffic-counts-road]"
  function
  | Empty -> roads
  | cond ->
      let ids = Storage.readDistinct connStr "[RoadID]" "gb-road-traffic-counts-daily-measurement" cond |> HashSet
      roads |> Array.filter (fst >> ids.Contains)

let filterYears = 
  let years = [| for y in 2000 .. 2015 -> box y, string y |]
  function
  | Empty -> years
  | cond ->
      let ids = Storage.readDistinct connStr "[Year]" "gb-road-traffic-counts-daily-measurement" cond |> HashSet
      years |> Array.filter (fst >> ids.Contains)

let filterDates cond = 
  let ids = Storage.readDistinct connStr "[Month]*1000+[Day]" "gb-road-traffic-counts-daily-measurement" cond 
  ids |> Array.map (fun encoded -> 
    let m = (encoded :?> int) / 1000
    let d = (encoded :?> int) - m * 1000
    encoded, sprintf "%s %d" (System.Globalization.DateTimeFormatInfo.InvariantInfo.GetMonthName(m)) d )

let facets = 
  [ ( box "year", "by year",
      Choice(fun filters -> 
        [ for y, name in filterYears filters -> 
            y, name, Filter(Compare(Column "Year", Equals, y)) ]))
    ( box "date", "by date",
      Choice(fun filters ->
        [ for y, name in filterYears filters -> 
            let cond = And(filters, Compare(Column "Year", Equals, y))
            y, name, Choice(fun filters ->            
              [ for enc, name in filterDates cond ->   
                  let cond = And(Compare(UncheckedExpression "[Month]*1000+[Day]", Equals, enc), cond)
                  enc, name, Filter(cond) ]) ]) )
    ( box "location", "by location",
      Choice(fun filters -> 
        [ for id, name in filterLocations filters -> 
            id, name, Filter(Compare(Column "LocationID", Equals, id)) ]))
    ( box "road", "by road",
      Choice(fun filters -> 
        [ for id, name in filterRoads filters -> 
            id, name, Filter(Compare(Column "RoadID", Equals, id)) ]))
    ( box "region", "by region",
      Choice(fun filters -> 
        [ for id, name in filterRegions filters -> 
            id, name, Filter(Compare(Column "RegionID", Equals, id)) ])) ]


// ------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators
open GovUk

let getDataMembers trace applied = seq {
  let applied = set applied
  if applied.Contains("road") || applied.Contains("location") || applied.Contains("region") then
    yield Property("get cyclist per day", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "cyc"; "day" |])
    yield Property("get motor vehicles per day", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "mv"; "day" |])
    yield Property("get cyclist per month", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "cyc"; "month" |])
    yield Property("get motor vehicles per month", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "mv"; "month" |])
    yield Property("get cyclist per year", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "cyc"; "year" |])
    yield Property("get motor vehicles per year", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "mv"; "year" |])
  if applied.Contains("year") || applied.Contains("date") then
    yield Property("get cyclist per road", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "cyc"; "road" |])
    yield Property("get motor vehicles per road", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "mv"; "road" |])
    yield Property("get cyclist per location", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "cyc"; "loc" |])
    yield Property("get motor vehicles per location", Primitive(Seq(Tuple(Named "date", Named "float")), "/data"), [| trace; "mv"; "loc" |]) }

open FSharp.Data
open Facets

let allDays =
  [ for d in 0.0 ..(DateTime(2015,12,31) - DateTime(2000,1,1)).TotalDays ->
      DateTimeOffset(2000,1,1,0,0,0,TimeSpan.Zero).AddDays(d) ]

let app = 
  Facets.createFacetedApp facets getDataMembers (fun args filter ->  
    let value, agg =
      match args with
      | ["cyc"; k] -> "CAST([PedalCycles] as float)", k
      | ["mv"; k] -> "CAST([MotorVehicles] as float)", k
      | _ -> failwith "Unexpected source or aggregation kind"
    
    let getData facet keyeq =    
      // Read data into a frame with dates as row keys & facet as column key
      let frame = 
        Storage.readFacetedTimeSeriesData connStr facet
          (value + ",[Year],[Month],[Day]") "[gb-road-traffic-counts-daily-measurement]" filter 
        |> Seq.groupBy (fun (rd, _, _) -> rd)
        |> Seq.map (fun (rd, grp) -> rd => series [for _, dt, v in grp -> dt => v])
        |> Frame.ofColumns      

      // Fill missing data and calculate sum across all facets
      // then resample data for all days
      let series =
        frame
        |> Frame.mapCols (fun _ -> Series.fillMissing Direction.Forward >> Series.fillMissing Direction.Backward)
        |> Frame.transpose
        |> Stats.sum
        |> Series.sample allDays
        |> Series.fillMissing Direction.Backward
        |> Series.chunkWhileInto keyeq Stats.mean

      // Apply some level of smoothing on the data      
      let bnd = max 1 (min 50 (series.KeyCount / 30)), Boundary.AtBeginning 
      series |> Series.windowSizeInto bnd (fun seg -> Stats.mean seg.Data)

    let data = 
      match agg with
      | "day" -> getData "[RoadID]" (fun d1 d2 -> (d1.Year,d1.Month,d1.Day) = (d2.Year,d2.Month,d2.Day)) 
      | "month" -> getData "[RoadID]" (fun d1 d2 -> (d1.Year,d1.Month) = (d2.Year,d2.Month)) 
      | "year" -> getData "[RoadID]" (fun d1 d2 -> d1.Year = d2.Year) 
      | "road"
      | "loc"
      | _ -> failwith "Unexpected aggregation kind"   

    [| for k, v in Series.observations data -> JsonValue.Array [| JsonValue.String (k.ToString("o")); JsonValue.Float v |] |]
    |> JsonValue.Array )
