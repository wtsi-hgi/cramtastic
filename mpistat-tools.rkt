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
         racket/stream
         racket/string
         "base64-suffix.rkt"
         "getent-group.rkt")


(provide
  (struct-out mpistat)

  (contract-out
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
                        void?))

    (mpistat-decode (-> (stream/c mpistat?)))))


;; Read an input port linewise, in the given mode, into a stream
(define (port->stream (in (current-input-port)) (mode 'linefeed))
  (define next-line (read-line in mode))
  (match next-line
    ((? eof-object?) empty-stream)
    (_               (stream-cons next-line (port->stream in mode)))))


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
      (match* (path/base64 path)
        (((? void?)        (? void?))        (void))
        (((? procedure? p) (? void?))        p)
        (((? void?)        (? procedure? p)) (compose p base64-decode/string))

        ; If there are predicates for both the encoded and plain path,
        ; then we test the encoded path first as an optimisation
        (((? procedure? p) (? procedure? q))
          (lambda (encoded-path)
            (and (p encoded-path) ((compose q base64-decode/string) encoded-path))))))

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

    ; Output matching lines
    (stream-for-each displayln
      (stream-filter record-match? (port->stream))))


;; mpistat record structure
(struct mpistat
  (path size uid gid atime mtime ctime mode inode-id hardlinks device-id))

;; Decode the mpistat records, taken from the current input port, into
;; a stream of mpistat structures
(define (mpistat-decode)
  ; Decode path
  (define decode-path (compose string->path base64-decode/string))

  ; Decode UID
  ; TODO Extend getent interface to include the passwd database
  (define decode-uid string->number)

  ; Decode GID
  (define decode-gid (compose first getent-group))

  ; Decode Unix time
  (define decode-time (compose (curryr seconds->date #f) string->number))

  ; Decode mode
  (define (decode-mode mode) (match mode ("f"  'file)
                                         ("d"  'directory)
                                         ("l"  'symlink)
                                         ("s"  'socket)
                                         ("b"  'block-device)
                                         ("c"  'character-device)
                                         ("F"  'named-pipe)
                                         ("X"  'other)))
  (define record-decoders
    (list decode-path string->number decode-uid decode-gid decode-time decode-time decode-time decode-mode string->number string->number string->number))

  ; Decode the data-decoder tuple
  (define (field-decode data/decoder)
    (match-define (cons data decoder) data/decoder)
    (decoder data))

  (define (record-decode mpistat-record)
    ; Zip the tab-delimited record with the list of decoders and execute
    ; the decoding. This gives us a list of decoded fields, which are
    ; then applied to the mpistat structure.
    (apply mpistat
      (map field-decode
           (map cons (string-split mpistat-record "\t") record-decoders))))

  (stream-map record-decode (port->stream)))


(module+ main
  (require "filter-predicates.rkt"
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
          (lambda () (mpistat-filter #:gid         (group-match? group-name)
                                     #:path/base64 (path/base64-suffix-match? suffix)))))))

  (close-input-port mpistat-input))


(module+ test
  (require racket/format
           racket/port
           rackunit)

  (define mpistat-build
    (let ((tab-delimit      (curryr string-replace #px"[[:blank:]]+" "\t"))
          (trailing-newline (curryr string-append "\n")))
      (compose trailing-newline tab-delimit ~a)))

  (define mpistats @mpistat-build{
    @; Base64-Encoded Path                    Size  UID  GID  atime  mtime  ctime  Mode  inode ID  # Hardlinks  Device ID
    @base64-encode/string{/path/to/file}       123  456  789      0      1      2  f     1                   1  1
    @base64-encode/string{/path/to/foo.bam}    456  789  123      1      2      0  l     2                   1  2
    @base64-encode/string{/path/to/no-where}   789  123  456      2      0      1  f     3                   2  1
    @base64-encode/string{/rooty/mcroot/face}  123    0    0      0      1      2  f     111               222  333
    @base64-encode/string{/foo/bar}            999    0    0    999    999    999  d     999               999  999})

  ; The above, linewise
  (define mpistats-lines (string-split mpistats #px"(?<=\n)"))

  ; Check port->stream works
  (let ((reconstructed (stream-fold (curryr string-append "\n") ""
                                    (with-input-from-string mpistats port->stream))))
    (check-equal? reconstructed mpistats))

  ; Wrapper around mpistat-filter that takes an optional positional
  ; argument of a string containing mpistat data (defaults to the test
  ; data defined above) and optional keyword arguments which will be
  ; passed as the filter predicates to mpistat-filter. The filtered
  ; output will be returned as a string
  (define test-filter
    (make-keyword-procedure
      (lambda (filter-kws filters . positional)
        (define input-string
          (match positional
            (empty              mpistats)
            ((list input _ ...) input)))

        (with-input-from-string input-string
          (lambda () (with-output-to-string
            (lambda ()
              (keyword-apply mpistat-filter filter-kws filters empty))))))))

  ; Wrapper around mpistat-decode that takes input from a string
  (define (test-decode input-string)
    (with-input-from-string input-string mpistat-decode))

  ; All match
  (check-equal? (test-filter) mpistats)

  ; No match
  (check-equal? (test-filter #:uid (lambda (_) #f)) "")

  ; Plaintext path
  (check-equal?
    (test-filter #:path (curry equal? "/path/to/file"))
    (first mpistats-lines))

  ; Encoded path
  (check-equal?
    (test-filter #:path/base64 (curry equal? @base64-encode/string{/path/to/foo.bam}))
    (second mpistats-lines))

  ; Plain and encoded path
  (check-equal?
    (test-filter #:path (curry equal? "/path/to/file")
                 #:path/base64 (curry equal? @base64-encode/string{/path/to/file}))
    (first mpistats-lines))

  ; Size
  (check-equal?
    (test-filter #:size (curry equal? "789"))
    (third mpistats-lines))

  ; UID
  (check-equal?
    (test-filter #:uid (curry equal? "789"))
    (second mpistats-lines))

  ; GID
  (check-equal?
    (test-filter #:gid (curry equal? "789"))
    (first mpistats-lines))

  ; atime
  (check-equal?
    (test-filter #:atime (curry equal? "2"))
    (third mpistats-lines))

  ; mtime
  (check-equal?
    (test-filter #:mtime (curry equal? "2"))
    (second mpistats-lines))

  ; ctime
  (check-equal?
    (test-filter #:ctime (compose zero? string->number))
    (second mpistats-lines))

  ; Mode
  (check-equal?
    (test-filter #:mode (curry equal? "l"))
    (second mpistats-lines))

  ; inode ID
  (check-equal?
    (test-filter #:inode-id (curry equal? "1"))
    (first mpistats-lines))

  ; Hardlinks (return multiple)
  (check-equal?
    (test-filter #:hardlinks (curry equal? "1"))
    (string-join (list (first mpistats-lines) (second mpistats-lines)) ""))

  ; Device ID (return multiple)
  (check-equal?
    (test-filter #:device-id (curry equal? "1"))
    (string-join (list (first mpistats-lines) (third mpistats-lines)) ""))

  ; Combined/non-trivial
  (check-equal?
    (test-filter #:path (curryr string-prefix? "/path/to")
                 #:device-id (curry equal? "1")
                 #:hardlinks (lambda (x) (< (string->number x) 2)))
    (first mpistats-lines))

  ; Check decoding
  ; n.b., We need to limit ourselves to root:root, because that's the
  ; only user and group we can guarantee
  (define decoded-stream
    (let ((root? (curry equal? "0")))
      (test-decode (test-filter #:uid root? #:gid root?))))

  (check-equal? (stream-length decoded-stream) 2)

  (let ((head (stream-first decoded-stream)))
    (check-equal?             (mpistat-path      head)  (string->path "/rooty/mcroot/face"))
    (check-equal?             (mpistat-size      head)  123)
    (check-equal?             (mpistat-uid       head)  0)
    (check-equal? (group-name (mpistat-gid       head)) "root")
    (check-equal?             (mpistat-atime     head)  (seconds->date 0 #f))
    (check-equal?             (mpistat-mtime     head)  (seconds->date 1 #f))
    (check-equal?             (mpistat-ctime     head)  (seconds->date 2 #f))
    (check-equal?             (mpistat-mode      head)  'file)
    (check-equal?             (mpistat-inode-id  head)  111)
    (check-equal?             (mpistat-hardlinks head)  222)
    (check-equal?             (mpistat-device-id head)  333))

  (check-equal?
    (stream->list (stream-map mpistat-mode decoded-stream))
    '(file directory)))
