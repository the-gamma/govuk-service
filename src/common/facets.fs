namespace GovUk


open System
open System.IO
open System.Collections.Generic

open Suave
open Suave.Filters
open Suave.Operators

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

type Filter = SqlExpression

type Facet = 
  | Filter of Filter
  | Choice of (Filter -> list<obj * string * Facet>)

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

open FSharp.Data

module Facets = 
  let (</>) (a:string) (b:string) = a.TrimEnd('/') + "/" + b.TrimStart('/')

  let rec findFacet filter (facets:list<obj * string * Facet>) pathHead pathTail =
    let facet = facets |> Seq.pick (fun (id, _, f) -> if pathHead = string id then Some f else None)
    match pathTail, facet with
    | (_, Filter _ | [], _) -> facet
    | p::ps, Choice f -> findFacet filter (f filter) p ps

  let createFacetedApp facets getDataMembers getData = 
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

    choose [
      path "/data" >=> request (fun r ->
        let trace = Utils.ASCII.toString(r.rawForm).Split('&') |> List.ofArray
        match List.head trace with
        | Filtering _ -> RequestErrors.BAD_REQUEST "Incomplete query specified"
        | Filtered (_, filter) ->
            let result = getData (List.tail trace) filter
            Successful.OK (result.ToString()) ) 
      request (fun r -> 
        match r.url.LocalPath with
        | Filtered(applied, filter) ->
            Serializer.returnMembers [ 
              let trace = r.url.LocalPath.Trim('/')
              yield! getDataMembers trace applied 
              for id, name, _ in facets do
                if applied |> Seq.exists ((=) (string id)) |> not then 
                  yield Property(name, Nested(r.url.LocalPath </> "filter" </> string id), [| |]) ] 
        | Filtering(facets) ->
            Serializer.returnMembers [
              for id, name, _ in facets ->
                Property(name, Nested(r.url.LocalPath </> string id), [| |])
            ]
      ) ]