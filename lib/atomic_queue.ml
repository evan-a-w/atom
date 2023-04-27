open Core

type id = int

let slots_per_chunk = 32

type 'a slot = 'a Option.t

type 'a chunk = {
  next : 'a chunk Option.t Atomic.t;
  slots : 'a slot Atomic.t Array.t;
}

let offset id = id mod slots_per_chunk

type 'a position = { chunk : 'a chunk; id : id }
type 'a queue = { head : 'a position Atomic.t; tail : 'a position Atomic.t }

let wait_till_next_chunk ~chunk =
  let backoff = Backoff.create () in
  let rec loop () =
    match Atomic.get chunk.next with
    | None ->
        Backoff.backoff backoff;
        loop ()
    | Some x -> x
  in
  loop ()

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

let write_to_chunk ~chunk ~offset x = Atomic.set chunk.slots.(offset) (Some x)

let pop queue =
  let backoff = Backoff.create () in
  let rec loop () =
    let head = Atomic.get queue.head in
    match phys_equal head.id (Atomic.get queue.tail).id with
    | true -> None
    | false -> (
        let new_head =
          if 0 = offset (head.id + 1) then
            (* if tail + 1 is on new chunk in push, preallocate it so that this works *)
            { chunk = wait_till_next_chunk ~chunk:head.chunk; id = head.id + 1 }
          else { head with id = head.id + 1 }
        in
        match Atomic.compare_and_set queue.head head new_head with
        | false ->
            Backoff.backoff backoff;
            loop ()
        | true -> wait_till_written ~chunk:head.chunk ~offset:(offset head.id))
  in
  loop ()

let create_chunk () =
  {
    next = Atomic.make None;
    slots = Array.init slots_per_chunk ~f:(fun _ -> Atomic.make None);
  }

let push queue x =
  let backoff = Backoff.create () in
  let rec loop () =
    let tail = Atomic.get queue.tail in
    let new_tail =
      if 0 = offset (tail.id + 1) then
        (* if tail + 1 is on new chunk in push, preallocate it so that this works *)
        { chunk = wait_till_next_chunk ~chunk:tail.chunk; id = tail.id + 1 }
      else { tail with id = tail.id + 1 }
    in
    match Atomic.compare_and_set queue.tail tail new_tail with
    | false ->
        Backoff.backoff backoff;
        loop ()
    | true ->
        write_to_chunk ~chunk:tail.chunk ~offset:(offset tail.id) x;
        if 0 = offset (tail.id + 1) then
          (* if tail + 1 is on new chunk preallocate it *)
          Atomic.set tail.chunk.next (Some (create_chunk ()))
  in
  loop ()

let create () =
  let head_chunk = create_chunk () in
  let queue =
    {
      head = Atomic.make { chunk = head_chunk; id = 0 };
      tail = Atomic.make { chunk = head_chunk; id = 0 };
    }
  in
  queue
