open Core

let add_to_queue ?counter ?(start_i = 0) queue ~n =
  let thread_id : int = Caml.Domain.self () |> Obj.magic in
  for i = start_i to n + start_i do
    Atomic_queue.push queue (i, thread_id);
    match counter with None -> () | Some counter -> Atomic.incr counter
  done

let pop_to_list ?(max_len = Int.max_value) queue =
  let rec loop n acc =
    if n >= max_len then acc
    else
      match Atomic_queue.pop queue with
      | None -> acc
      | Some x -> loop (n + 1) (x :: acc)
  in
  loop 0 [] |> List.rev

let%expect_test "single thread" =
  let queue = Atomic_queue.create () in
  add_to_queue queue ~n:100;
  let list = pop_to_list queue in
  print_s [%sexp (list : (int * int) list)];
  [%expect
    {|
    ((0 0) (1 0) (2 0) (3 0) (4 0) (5 0) (6 0) (7 0) (8 0) (9 0) (10 0) (11 0)
     (12 0) (13 0) (14 0) (15 0) (16 0) (17 0) (18 0) (19 0) (20 0) (21 0)
     (22 0) (23 0) (24 0) (25 0) (26 0) (27 0) (28 0) (29 0) (30 0) (31 0)
     (32 0) (33 0) (34 0) (35 0) (36 0) (37 0) (38 0) (39 0) (40 0) (41 0)
     (42 0) (43 0) (44 0) (45 0) (46 0) (47 0) (48 0) (49 0) (50 0) (51 0)
     (52 0) (53 0) (54 0) (55 0) (56 0) (57 0) (58 0) (59 0) (60 0) (61 0)
     (62 0) (63 0) (64 0) (65 0) (66 0) (67 0) (68 0) (69 0) (70 0) (71 0)
     (72 0) (73 0) (74 0) (75 0) (76 0) (77 0) (78 0) (79 0) (80 0) (81 0)
     (82 0) (83 0) (84 0) (85 0) (86 0) (87 0) (88 0) (89 0) (90 0) (91 0)
     (92 0) (93 0) (94 0) (95 0) (96 0) (97 0) (98 0) (99 0) (100 0)) |}]

let%expect_test "single thread interleave" =
  let queue = Atomic_queue.create () in
  add_to_queue queue ~n:33;
  let list = pop_to_list ~max_len:10 queue in
  print_s [%sexp (list : (int * int) list)];
  add_to_queue queue ~start_i:33 ~n:34;
  let list = pop_to_list ~max_len:20 queue in
  print_s [%sexp (list : (int * int) list)];
  let list = pop_to_list queue in
  print_s [%sexp (list : (int * int) list)];
  [%expect
    {|
    ((0 0) (1 0) (2 0) (3 0) (4 0) (5 0) (6 0) (7 0) (8 0) (9 0))
    ((10 0) (11 0) (12 0) (13 0) (14 0) (15 0) (16 0) (17 0) (18 0) (19 0)
     (20 0) (21 0) (22 0) (23 0) (24 0) (25 0) (26 0) (27 0) (28 0) (29 0))
    ((30 0) (31 0) (32 0) (33 0) (33 0) (34 0) (35 0) (36 0) (37 0) (38 0)
     (39 0) (40 0) (41 0) (42 0) (43 0) (44 0) (45 0) (46 0) (47 0) (48 0)
     (49 0) (50 0) (51 0) (52 0) (53 0) (54 0) (55 0) (56 0) (57 0) (58 0)
     (59 0) (60 0) (61 0) (62 0) (63 0) (64 0) (65 0) (66 0) (67 0)) |}]

let%test_unit "multi thread" =
  let queue = Atomic_queue.create () in
  let res_queue = Atomic_queue.create () in
  let num_written = Atomic.make 0 in
  let num_writers = Domain.recommended_domain_count () in
  let writers =
    Array.init num_writers ~f:(fun id ->
        Domain.spawn (fun () ->
            let start_i = id * 100 in
            for i = 0 to 9 do
              add_to_queue ~counter:num_written queue
                ~start_i:(start_i + (i * 10))
                ~n:9;
              for _ = 0 to 9 do
                match Atomic_queue.pop queue with
                | None -> ()
                | Some x -> Atomic_queue.push res_queue x
              done
            done))
  in
  Array.iter writers ~f:Domain.join;
  pop_to_list queue |> List.iter ~f:(fun x -> Atomic_queue.push res_queue x);
  let res_list =
    pop_to_list res_queue
    |> List.sort ~compare:(fun (i, _) (j, _) -> Int.compare i j)
    |> List.map ~f:Tuple2.get1
  in
  [%test_eq: int List.t] res_list
    (List.init (num_writers * 100) ~f:(fun i -> i))
