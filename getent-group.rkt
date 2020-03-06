;;; getent group database interface

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/port
         racket/string
         racket/system)


(provide/contract
  (getent-group (-> string? ... (listof string?)))
  (group->gid   (-> string? exact-nonnegative-integer?)))


;; getent system call
(define getent
  (let ((/path/to/getent (or (find-executable-path "getent")
                             (error "getent not available"))))
    (curry system* /path/to/getent)))


;; Wrapper for `getent group $@ 2>/dev/null`
; TODO Return a list of structs rather than raw strings
(define (getent-group . groups)
  (define output (open-output-string))

  ; TODO Use getent's documented exit codes
  (unless
    (parameterize ((current-output-port output)
                   (current-error-port  (open-output-nowhere)))
      (apply getent "group" groups))

    (error "getent call failed"))

  (string-split (get-output-string output) "\n"))


;; Map group name to GID
(define (group->gid group)
  (define getent-record (getent-group group))
  (when (zero? (length getent-record))
    (error "Cannot map to group ID: Group record not found"))

  (string->number (third (string-split (first getent-record) ":"))))
