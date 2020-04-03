#!/usr/bin/env racket

;;; Find the common base64 suffices of a given plaintext suffix

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/set
         racket/vector
         net/base64)


(provide/contract
  (base64-decode/string (-> string? string?))
  (base64-encode/string (-> string? string?))
  (base64-suffices (->* (string?)
                        (exact-positive-integer?)
                        (values string? string? string?))))


(define (string->bytes->string fn)
  (compose bytes->string/utf-8 fn string->bytes/utf-8))

;; Base64 decode/encode on strings, rather than bytes
(define base64-decode/string (string->bytes->string base64-decode))
(define base64-encode/string (string->bytes->string (curryr base64-encode #"")))


;; Generate a random string of a given length, where each character
;; matches the optional regular expression
(define (random-string n (pattern #px"[[:alnum:]]"))
  ; Random 8-bit ASCII character
  (define (random-char) (integer->char (random 0 256)))

  ; Check character matches pattern
  (define (char-match? char) (regexp-match? pattern (string char)))

  ; Recursively build list of n matching characters
  (define (builder (built '()))
    (cond
      ((equal? n (length built)) built)
      (else
        (define next-char (random-char))
        (define new-build
          (cond
            ((char-match? next-char) (cons next-char built))
            (else                    built)))

        (builder new-build))))

  (list->string (builder)))


;; Find the common suffix of the given strings
(define (common-suffix . strings)
  ; Find the minimum length of all the strings for trimming
  (define trim-length (apply min (map string-length strings)))

  ; Trim the strings from the right and zip their characters together
  (define zipped-suffices
    (apply map list
      (map
        (compose (curryr take-right trim-length)
                 string->list)
        strings)))

  ; Are all elements of the list the same?
  (define (homogeneous-list? haystack)
    (or (empty? haystack)
        (let* ((needle     (first haystack))
               (is-needle? (curry equal? needle)))
          (andmap is-needle? haystack))))

  (list->string
    (map first (takef-right zipped-suffices homogeneous-list?))))


;; Find the common base64 suffices of the given plaintext suffix
(define (base64-suffices plaintext-suffix (trials 10))
  (define sample-results
    ; base64 padding has three possible alignments per trial
    (let* ((alignments 3)
           (samples    (* alignments trials)))

      (map
        (lambda (alignment)
          (map
            ; Generate sample: Random string of given length, appended
            ; with the plaintext suffix, then base64 encoded
            (compose base64-encode/string
                     (curryr string-append plaintext-suffix)
                     random-string)

            (range alignment samples alignments)))
        (range alignments))))

  ; Calculate the common suffix for each padding alignment
  (apply values
    (map
      (curry apply common-suffix)
      sample-results)))


(module+ main
  ; Get suffix from command line
  (define suffix
    (let ((argv (current-command-line-arguments)))
      (cond
        ((vector-empty? argv) ".bam")
        (else                 (vector-ref argv 0)))))

  ; Get number of trials from environment
  (define trials
    (string->number
      (or (getenv "TRIALS") "10")))

  (displayln
    (format "Common base64 suffices for ~s:" suffix)
    (current-error-port))

  (let-values (((sfx1 sfx2 sfx3) (base64-suffices suffix trials)))
    (displayln (format "~a~n~a~n~a" sfx1 sfx2 sfx3))))


(module+ test
  (require rackunit)

  (check-equal? (base64-decode/string "SGVsbG8=") "Hello")
  (check-equal? (base64-encode/string "Hello") "SGVsbG8=")

  (check-equal? (string-length (random-string 50)) 50)
  (check-regexp-match #px"^[[:alnum:]]{10}$" (random-string 10))

  (check-equal? (common-suffix "Monday" "Tuesday" "Wednesday") "day")
  (check-equal? (common-suffix "January" "February" "March")   "")

  (let-values (((bam1  bam2  bam3)  (base64-suffices ".bam"))
               ((cram1 cram2 cram3) (base64-suffices ".cram"))
               ((test1 test2 test3) (base64-suffices "test")))

    (check-equal? (set bam1  bam2  bam3)  (set "uYmFt"  "5iYW0="   "LmJhbQ=="))
    (check-equal? (set cram1 cram2 cram3) (set "5jcmFt" "LmNyYW0=" "uY3JhbQ=="))
    (check-equal? (set test1 test2 test3) (set "0ZXN0"  "Rlc3Q="   "dGVzdA=="))))
