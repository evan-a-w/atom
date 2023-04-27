type t = { mutable loops : int }

let loops_max = 1000
let create () = { loops = 1 }

let backoff t =
  if t.loops >= loops_max then Unix.sleepf (float t.loops /. 1000.0)
  else (
    for _ = 1 to t.loops do
      Caml.Domain.cpu_relax ()
    done;
    t.loops <- Int.min (t.loops * 2) loops_max)
