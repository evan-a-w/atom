type 'a t

val pop : 'a t -> 'a option
val push : 'a t -> 'a -> unit
val create : unit -> 'a t
