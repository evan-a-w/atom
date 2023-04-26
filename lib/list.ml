open Core

type 'a t = {
  id : int;
  value: 'a;
  next : 'a t option;
} [@@deriving sexp]
