type id = int

let slots_per_chunk = 32

type 'a slot = 'a Option.t

type 'a chunk = {
  next : 'a chunk Option.t Atomic.t;
  slots : 'a slot Atomic.t Array.t;
}

type 'a queue = {
  head : id Atomic.t;
  head_chunk : 'a chunk Atomic.t;
  tail : id Atomic.t;
  tail_chunk : 'a chunk Atomic.t;
}

let wait_till_written ~chunk ~offset =
  let backoff = Backoff.create () in
  let rec loop () =
    match Atomic.get chunk.slots.(offset) with
    | None ->
        Backoff.backoff backoff;
        loop ()
    | Some _ as x -> x
  in
  loop ()

let pop queue =
  let rec loop () =
    let head = Atomic.get queue.head in
    match head = Atomic.get queue.tail with
    | true -> None
    | false -> (
        match Atomic.compare_and_set queue.head head (head + 1) with
        | false -> loop ()
        | true ->
            let head_chunk = Atomic.get queue.head_chunk in
            wait_till_written ~chunk:head_chunk
              ~offset:(head mod slots_per_chunk))
  in
  loop ()
