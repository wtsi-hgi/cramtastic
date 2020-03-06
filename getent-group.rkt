;;; getent group database interface

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/match
         racket/port
         racket/string
         racket/system)


(provide
  (struct-out group)
  (contract-out
    (getent-group (-> string? ... (listof group?)))
    (group->gid   (-> string? exact-nonnegative-integer?))))


;; getent system call
(define getent
  (let ((/path/to/getent (or (find-executable-path "getent")
                             (error "getent not available"))))
    (curry system* /path/to/getent)))


;; Group database record
(struct group (name password gid users))


;; Wrapper for `getent group $@ 2>/dev/null`
(define (getent-group . groups)
  (define output (open-output-string))

  ; TODO Use getent's documented exit codes
  (unless
    (parameterize ((current-output-port output)
                   (current-error-port  (open-output-nowhere)))
      (apply getent "group" groups))

    (error "getent call failed"))

  ; Map each string record into group structure
  (map
    (lambda (record)
      (match (string-split record ":")
        ((list name password gid users) (group name
                                               password
                                               (string->number gid)
                                               (string-split users ",")))))

    (string-split (get-output-string output) "\n")))


;; Map group name to GID
(define (group->gid group)
  (group-gid (first (getent-group group))))
