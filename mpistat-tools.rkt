#!/usr/bin/env racket

;;; Filter and decode mpistat data

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang at-exp racket/base

(require racket/contract
         racket/function
         racket/list
         racket/match
         racket/string
         net/base64)


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

  ; Get gzip buffer size from environment (defaults to 16KiB)
  (define gzip-buffer
    (string->number (or (getenv "GZIP_BUFFER") "16384")))

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
      ((terminal-port? (current-output-port))
        (curryr apply empty))
      (else
        (curry with-gzip #:buffer-size gzip-buffer))))

  ; Stream through the input and filter
  (parameterize ((current-input-port mpistat-input))
    (with-appropriate-output
      (lambda ()
        (with-gunzip #:buffer-size gzip-buffer
          (lambda () (mpistat-filter #:gid         gid-match?
                                     #:path/base64 suffix-match?))))))

  (close-input-port mpistat-input))


(module+ test
  (require racket/format
           racket/port
           rackunit)

  (define trailing-newline (curryr string-append "\n"))

  (define mpistat-build
    (let ((tab-delimit      (curryr string-replace #px"[[:blank:]]+" "\t")))
      (compose trailing-newline tab-delimit ~a)))

  (define path ; i.e., base64-encode/string
    (compose bytes->string/utf-8 (curryr base64-encode #"") string->bytes/utf-8))

  (define mpistats @mpistat-build{
    @; Base64-Encoded Path     Size  UID  GID  atime  mtime  ctime  Mode  inode ID  # Hardlinks  Device ID
    @path{/path/to/file}        123  456  789      0      1      2  f     1                   1  1
    @path{/path/to/foo.bam}     456  789  123      1      2      0  l     2                   1  2
    @path{/path/to/no-where}    789  123  456      2      0      1  f     3                   2  1})

  ; The above, linewise
  (define mpistats-lines (map trailing-newline (string-split mpistats "\n")))

  ; Wrapper around mpistat-filter that takes an optional positional
  ; argument of a string containing mpistat data (defaults to the test
  ; data defined above) and optional keyword arguments which will be
  ; passed as the filter predicates to mpistat-filter. The filtered
  ; output will be returned as a string
  (define test-filter
    (make-keyword-procedure
      (lambda (filter-kws filters . positional)
        (define input-string
          (cond ((empty? positional) mpistats)
                (else                (first positional))))

        (with-input-from-string input-string
          (lambda () (with-output-to-string
            (lambda ()
              (keyword-apply mpistat-filter filter-kws filters '()))))))))

  ; All match
  (check-equal? (test-filter) mpistats)

  ; No match
  (check-equal? (test-filter #:uid (lambda (_) #f)) "")

  ; Plaintext path
  (check-equal?
    (test-filter #:path (lambda (x) (equal? x "/path/to/file")))
    (first mpistats-lines))

  ; Encoded path
  (check-equal?
    (test-filter #:path/base64 (lambda (x) (equal? x @path{/path/to/foo.bam})))
    (second mpistats-lines))

  ; Plain and encoded path
  (check-equal?
    (test-filter #:path (lambda (x) (equal? x "/path/to/file"))
                 #:path/base64 (lambda (x) (equal? x @path{/path/to/file})))
    (first mpistats-lines))

  ; Size
  (check-equal?
    (test-filter #:size (lambda (x) (equal? x "789")))
    (third mpistats-lines))

  ; UID
  (check-equal?
    (test-filter #:uid (lambda (x) (equal? x "789")))
    (second mpistats-lines))

  ; GID
  (check-equal?
    (test-filter #:gid (lambda (x) (equal? x "789")))
    (first mpistats-lines))

  ; atime
  (check-equal?
    (test-filter #:atime (lambda (x) (> (string->number x) 1)))
    (third mpistats-lines))

  ; mtime
  (check-equal?
    (test-filter #:mtime (lambda (x) (equal? x "2")))
    (second mpistats-lines))

  ; ctime
  (check-equal?
    (test-filter #:ctime (lambda (x) (zero? (string->number x))))
    (second mpistats-lines))

  ; Mode
  (check-equal?
    (test-filter #:mode (lambda (x) (equal? x "l")))
    (second mpistats-lines))

  ; inode ID
  (check-equal?
    (test-filter #:inode-id (lambda (x) (equal? x "1")))
    (first mpistats-lines))

  ; Hardlinks (return multiple)
  (check-equal?
    (test-filter #:hardlinks (lambda (x) (equal? x "1")))
    (string-join (list (first mpistats-lines) (second mpistats-lines)) ""))

  ; Device ID (return multiple)
  (check-equal?
    (test-filter #:device-id (lambda (x) (equal? x "1")))
    (string-join (list (first mpistats-lines) (third mpistats-lines)) ""))

  ; Combined/non-trivial
  (check-equal?
    (test-filter #:path (lambda (x) (string-prefix? x "/path/to"))
                 #:device-id (lambda (x) (equal? x "1"))
                 #:hardlinks (lambda (x) (< (string->number x) 2)))
    (first mpistats-lines)))
