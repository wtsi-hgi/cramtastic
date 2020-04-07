;;; getent database interface

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
  (struct-out passwd)

  (contract-out
    (getent-group  (-> string? ... (listof group?)))
    (getent-passwd (-> string? ... (listof passwd?)))
    (group->gid    (-> string? exact-nonnegative-integer?))
    (gid->group    (-> exact-nonnegative-integer? string?))
    (user->uid     (-> string? exact-nonnegative-integer?))
    (uid->user     (-> exact-nonnegative-integer? string?))))


;; Group database record
(struct group (name password gid users))

;; Passwd database record
(struct passwd (username password uid gid gecos home shell))

;; Adaptor: Convert raw string input into the appropriate record struct
;; TODO This only works by virtue of the records having different field
;; counts for different databases; maybe make this a bit less hacky...
(define (raw->record raw)
  (match (string-split raw ":" #:trim? #f)
    ((list name password gid users)
     (group name
            password
            (string->number gid)
            (string-split users ",")))

    ((list username password uid gid gecos home shell)
     (passwd username
             password
             (string->number uid)
             (string->number gid)
             (string-split gecos ",")
             (string->path home)
             (string->path shell)))))


;; Wrapper for `getent $DATABASE $@ 2>/dev/null`
(define (getent database . keys)
  (define /path/to/getent (or (find-executable-path "getent")
                              (error "getent not available")))
  (define get-entries
    (parameterize ((current-error-port (open-output-nowhere)))
      (curry system* /path/to/getent database)))

  (define entries (with-output-to-string
    (lambda ()
      ; TODO Use getent's documented exit codes
      (unless (apply get-entries keys)
              (error "getent call failed")))))

  (map raw->record (string-split entries "\n")))


;; Exposed API
(define (getent-group . groups) (apply getent "group" groups))
(define (getent-passwd . users) (apply getent "passwd" users))

(define (group->gid group) (group-gid (first (getent-group group))))
(define (gid->group gid) (group-name (first (getent-group (number->string gid)))))

(define (user->uid user) (passwd-uid (first (getent-passwd user))))
(define (uid->user uid) (passwd-username (first (getent-passwd (number->string uid)))))


(module+ test
  (require rackunit)

  (let* ((root-search (getent-group "root"))
         (root-group  (first root-search)))
    (check-equal? (length root-search) 1)
    (check-equal? (group-name root-group) "root")
    (check-equal? (group-gid  root-group) 0))

  (let* ((root-search (getent-passwd "root"))
         (root-user   (first root-search)))
    (check-equal? (length root-search) 1)
    (check-equal? (passwd-username root-user) "root")
    (check-equal? (passwd-uid      root-user) 0))

  (check-equal? (gid->group (group->gid "root")) "root")
  (check-equal? (uid->user  (user->uid  "root")) "root"))
