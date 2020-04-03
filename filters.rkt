;;; Filter predicates for mpistat data

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         "base64-suffix.rkt"
         "getent-group.rkt")


(provide/contract
  (path-suffix-match? (-> string? predicate/c))
  (group-match?       (-> string? predicate/c)))


;; Predicate on path suffix match (base64 encoded, as an optimisation)
(define (path-suffix-match? suffix)
  (let*-values (((sfx1 sfx2 sfx3) (base64-suffices suffix))
                ((suffix-regexp)  (regexp (format "(~a|~a|~a)$" sfx1 sfx2 sfx3))))
    (curry regexp-match? suffix-regexp)))


;; Predicate on group match
(define (group-match? group-name)
  (let ((gid (number->string (group->gid group-name))))
    (curry equal? gid)))
