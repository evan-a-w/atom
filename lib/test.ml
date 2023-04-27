open Core

let add_to_queue ~queue ~n =
  let thread_id : int = Caml.Domain.self () |> Obj.magic in
  for i = 1 to n do
    Atomic_queue.push queue (i, thread_id)
  done

let pop_to_list ~queue =
  let rec loop acc =
    match Atomic_queue.pop queue with None -> acc | Some x -> loop (x :: acc)
  in
  loop []

let%expect_test "single thread" =
  let queue = Atomic_queue.create () in
  add_to_queue ~queue ~n:10;
  let list = pop_to_list ~queue in
  print_s [%sexp (list : (int * int) list)];
  [%expect {| ((10 1) (9 1) (8 1) (7 1) (6 1) (5 1) (4 1) (3 1) (2 1) (1 1)) |}]
