open Core

let wait_till_written ~atomic_option =
  let backoff = Backoff.create () in
  let rec loop () =
    match Atomic.get atomic_option with
    | None ->
        Backoff.backoff backoff;
        loop ()
    | Some _ as x -> x
  in
  loop ()

module Id = struct
  type t = int [@@deriving sexp]

  let equal = Int.equal
end

module Slot = struct
  type 'a t = 'a Option.t [@@deriving sexp_of]
end

module Chunk = struct
  type 'a t = {
    next : 'a t Option.t Atomic.t;
    slots : 'a Slot.t Atomic.t Array.t;
  }

  let slots_per_chunk = 32
  let offset (id : Id.t) = id mod slots_per_chunk
  let get t ~id = t.slots.(offset id)
  let set t ~id ~value = Atomic.set t.slots.(offset id) (Some value)

  let sexp_of_t sexp_of_a t =
    [%sexp (Array.map t.slots ~f:Atomic.get : a Slot.t array)]

  let create () =
    {
      next = Atomic.make None;
      slots = Array.init slots_per_chunk ~f:(fun _ -> Atomic.make None);
    }
end

module Position = struct
  type 'a t = { chunk : 'a Chunk.t; id : Id.t } [@@deriving sexp_of]
end

type 'a t = { head : 'a Position.t Atomic.t; tail : 'a Position.t Atomic.t }

let pop queue =
  let backoff = Backoff.create () in
  let rec loop () =
    let head = Atomic.get queue.head in
    match Id.equal head.id (Atomic.get queue.tail).id with
    | true -> None
    | false -> (
        let new_head =
          let id = head.id + 1 in
          if 0 = Chunk.offset id then
            (* if tail + 1 is on new chunk in push, preallocate it so that this works *)
            Position.
              {
                chunk =
                  wait_till_written ~atomic_option:head.chunk.next
                  |> Option.value_exn;
                id;
              }
          else { head with id }
        in
        match Atomic.compare_and_set queue.head head new_head with
        | false ->
            Backoff.backoff backoff;
            loop ()
        | true ->
            let atomic_option = Chunk.get head.chunk ~id:head.id in
            let res = wait_till_written ~atomic_option in
            Atomic.set atomic_option None;
            res)
  in
  loop ()

let push queue x =
  let backoff = Backoff.create () in
  let rec loop () =
    let tail = Atomic.get queue.tail in
    let new_tail =
      let id = tail.id + 1 in
      if 0 = Chunk.offset id then
        (* if tail + 1 is on new chunk in push, preallocate it so that this works *)
        Position.
          {
            chunk =
              wait_till_written ~atomic_option:tail.chunk.next
              |> Option.value_exn;
            id;
          }
      else { tail with id }
    in
    match Atomic.compare_and_set queue.tail tail new_tail with
    | false ->
        Backoff.backoff backoff;
        loop ()
    | true ->
        Chunk.set tail.chunk ~id:tail.id ~value:x;
        if 0 = Chunk.offset (new_tail.id + 1) then
          (* if tail + 1 is on new chunk preallocate it *)
          Atomic.set new_tail.chunk.next (Some (Chunk.create ()))
  in
  loop ()

let create () =
  let head_chunk = Chunk.create () in
  let queue =
    {
      head = Atomic.make Position.{ chunk = head_chunk; id = 0 };
      tail = Atomic.make Position.{ chunk = head_chunk; id = 0 };
    }
  in
  queue
