#!/usr/bin/env racket

;;; Filter mpistat data on the current input port

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/match
         racket/string
         net/base64)


; mpistat-filter predicate contract
(define predicate/c (-> string? boolean?))

(provide/contract
  (mpistat-filter (-> #:path        (or/c void? predicate/c)
                      #:path/base64 (or/c void? predicate/c)
                      #:size        (or/c void? predicate/c)
                      #:uid         (or/c void? predicate/c)
                      #:gid         (or/c void? predicate/c)
                      #:atime       (or/c void? predicate/c)
                      #:mtime       (or/c void? predicate/c)
                      #:ctime       (or/c void? predicate/c)
                      #:mode        (or/c void? predicate/c)
                      #:inode-id    (or/c void? predicate/c)
                      #:hardlinks   (or/c void? predicate/c)
                      #:device-id   (or/c void? predicate/c)
                      void?)))


;; Base64 decode a string
(define base64-decode/string
  (compose bytes->string/utf-8 base64-decode string->bytes/utf-8))


;; Filter the mpistat records, taken from the current input port, by the
;; provided field-based predicates
(define (mpistat-filter #:path        (path        (void))
                        #:path/base64 (path/base64 (void))
                        #:size        (size        (void))
                        #:uid         (uid         (void))
                        #:gid         (gid         (void))
                        #:atime       (atime       (void))
                        #:mtime       (mtime       (void))
                        #:ctime       (ctime       (void))
                        #:mode        (mode        (void))
                        #:inode-id    (inode-id    (void))
                        #:hardlinks   (hardlinks   (void))
                        #:device-id   (device-id   (void)))

    ; Normalise the path predicate
    (define path/universal
      (cond
        ((and (void? path/base64)      (void? path))      (void))
        ((and (procedure? path/base64) (void? path))      path/base64)
        ((and (void? path/base64)      (procedure? path)) (compose path base64-decode/string))

        ; If there are predicates for both the encoded and plain path,
        ; then we test the encoded path first as an optimisation
        ((and (procedure? path/base64) (procedure? path))
          (lambda (encoded-path)
            (and (path/base64                         encoded-path)
                 ((compose path base64-decode/string) encoded-path))))))

    (define record-filters
      (list path/universal size uid gid atime mtime ctime mode inode-id hardlinks device-id))

    ; n.b., Void predicates trivially evaluate to true
    (define (field-match? data/predicate)
      (match-define (cons data predicate) data/predicate)
      (or (void? predicate)
          (predicate data)))

    ; Fields and their respective predicates are zipped together, then
    ; each are checked; andmap will short-circuit if any field/predicate
    ; evaluates to false
    (define (record-match? mpistat-record)
      (define fields (string-split mpistat-record "\t"))
      (andmap field-match? (map cons fields record-filters)))

    ; Iterate through input, line-by-line
    (define (iterate-through input-port)
      (define mpistat-record (read-line input-port))

      (unless (eof-object? mpistat-record)
        (when (record-match? mpistat-record) (displayln mpistat-record))
        (iterate-through input-port)))

    (iterate-through (current-input-port)))


;; TODO mpistat decoding
;; Tag by filetype
(define (type-tag filetype)
  (match filetype
    ("f"  'file)
    ("d"  'directory)
    ("l"  'symlink)
    ("s"  'socket)
    ("b"  'block-device)
    ("c"  'character-device)
    ("F"  'named-pipe)
    ("X"  'other)))


(module+ main
  (require "getent-group.rkt"
           "base64-suffix.rkt"
           "with-gzip.rkt")

  ; Get input port, group name and suffix from command line
  (define-values (mpistat-input group-name suffix)
    (match (current-command-line-arguments)
      ('#()
       (error "Not enough arguments"))

      ((vector group)
       (values (current-input-port) group ".bam"))

      ((vector group suffix)
       (values (current-input-port) group suffix))

      ((vector "-" group suffix _ ...)
       (values (current-input-port) group suffix))

      ((vector file group suffix _ ...)
       (values (open-input-file file #:mode 'binary) group suffix))))

  ; Predicate on GID match
  (define gid-match?
    (let ((gid (number->string (group->gid group-name))))
      (curry equal? gid)))

  ; Predicate on suffix match
  (define suffix-match?
    (let*-values (((sfx1 sfx2 sfx3) (base64-suffices suffix))
                  ((suffix-regexp)  (regexp (format "(~a|~a|~a)$" sfx1 sfx2 sfx3))))
      (curry regexp-match? suffix-regexp)))

  ; Compress when not outputting to a TTY, otherwise a no-op
  (define with-appropriate-output
    (cond
      ((terminal-port? (current-output-port)) (curryr apply empty))
      (else                                   with-gzip)))

  ; Stream through the input and filter
  (parameterize ((current-input-port mpistat-input))
    (with-appropriate-output
      (lambda ()
        (with-gunzip
          (lambda () (mpistat-filter #:gid         gid-match?
                                     #:path/base64 suffix-match?))))))

  (close-input-port mpistat-input))


(module+ test
  (require rackunit)
  #;TODO )
