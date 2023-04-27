(lang dune 3.7)

(name atom)

(generate_opam_files true)

(source
 (github evan-a-w/atom))

(authors "Evan Williams")

(maintainers "Evan Williams")

(license MIT)

(package
 (name atom)
 (synopsis "Lock free data structures")
 (description "An implementation of 1. A lock free queue")
 (depends ocaml dune core unix)
 (tags
  (topics)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
