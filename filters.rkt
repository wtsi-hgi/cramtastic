;;; Filter predicates for mpistat data

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         "base64-suffix.rkt"
         "getent.rkt")


(provide/contract
  (path/base64-suffix-match? (-> string? predicate/c))
  (group-match?              (-> string? predicate/c))
  (owner-match?              (-> string? predicate/c)))


;; Predicate on path suffix match
(define (path/base64-suffix-match? suffix)
  (let*-values (((sfx1 sfx2 sfx3) (base64-suffices suffix))
                ((suffix-regexp)  (regexp (format "(~a|~a|~a)$" sfx1 sfx2 sfx3))))
    (curry regexp-match? suffix-regexp)))


;; Predicate on group match
(define (group-match? group-name)
  (let ((gid (number->string (group-gid (name->group group-name)))))
    (curry equal? gid)))


;; Predicate on owner match
(define (owner-match? username)
  (let ((uid (number->string (user-uid (name->user username)))))
    (curry equal? uid)))


(module+ test
  (require rackunit)

  (check-true ((path/base64-suffix-match? ".bam")
               (base64-encode/string "something.bam")))

  (check-false ((path/base64-suffix-match? ".bam")
                (base64-encode/string "foo")))

  (check-true ((group-match? "root") "0"))

  (check-true ((owner-match? "root") "0")))
