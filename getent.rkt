#!/usr/bin/env racket

;;; passwd and group database interface

; Written by Christopher Harrison <ch12@sanger.ac.uk>
; Copyright (c) 2020 Genome Research Ltd.
; Licensed under the terms of the GPLv3

#lang racket/base

(require racket/contract
         racket/function
         racket/list
         racket/match
         racket/struct
         (rename-in ffi/unsafe (-> -->)))


(provide
  (struct-out user)
  (struct-out group)

  (contract-out
    (username->user    (-> string? user?))
    (uid->user         (-> exact-nonnegative-integer? user?))
    (group-name->group (-> string? group?))
    (gid->group        (-> exact-nonnegative-integer? group?))))


;; C type definitions
(define _uid_t _uint)
(define _gid_t _uint)

;; The group C struct's gr_mem element is a NULL-terminated array of
;; strings; this pointer type recursively builds that out as a list.
(define-cpointer-type _string/list _pointer
  ; Racket-to-C: This should be treated as read-only, so we raise
  (lambda (_) (error "Cannot write to memory"))

  ; C-to-Racket
  (lambda (ptr)
    (define (build (built empty) (offset 0))
      (define group (ptr-ref ptr _string offset))
      (match group
        (#f          (reverse built))
        ((? string?) (build (cons group built) (+ offset 1)))))

    (build)))

; NOTE This is the Linux definition, which differs in BSD (e.g., macOS)
;      and, strictly, POSIX (which doesn't define pw_gecos)
(define-cstruct _c-passwd ((pw_name   _string)
                           (pw_passwd _string)
                           (pw_uid    _uid_t)
                           (pw_gid    _gid_t)
                           (pw_gecos  _string)
                           (pw_dir    _path)
                           (pw_shell  _path)))

(define-cstruct _c-group  ((gr_name   _string)
                           (gr_passwd _string)
                           (gr_gid    _gid_t)
                           (gr_mem    _string/list)))


;; User-facing structures
(struct user (username password uid gid gecos home-directory shell))
(struct group (group-name password gid members))


;; Wrapper for the FFI functions, which take one argument and return a
;; a NULL pointer on failure, raised as an exception
(define (libc-wrapper symbol input-type output-type)
  (define ffi-fn
    (get-ffi-obj symbol #f (_fun #:save-errno 'posix input-type --> output-type)))

  (lambda (arg) (or (ffi-fn arg)
                    (error 'FFI "~a failed [errno:~a]" symbol (saved-errno)))))

(define getpwnam (libc-wrapper "getpwnam" _string _c-passwd-pointer/null))
(define getpwuid (libc-wrapper "getpwuid" _uid_t  _c-passwd-pointer/null))
(define getgrnam (libc-wrapper "getgrnam" _string _c-group-pointer/null))
(define getgrgid (libc-wrapper "getgrgid" _gid_t  _c-group-pointer/null))

(define c-passwd->user (compose (curry apply user) c-passwd->list))
(define c-group->group (compose (curry apply group) c-group->list))


;; User-facing functions
(define username->user    (compose c-passwd->user getpwnam))
(define uid->user         (compose c-passwd->user getpwuid))
(define group-name->group (compose c-group->group getgrnam))
(define gid->group        (compose c-group->group getgrgid))


(module+ main
  (require racket/string)

  ; Very basic frontend; not a replacement for getent!
  (match (current-command-line-arguments)
    ((vector "passwd" username)
     (let ((user (username->user username)))
       (displayln (format "~a:~a:~a:~a:~a:~a:~a" (user-username       user)
                                                 (user-password       user)
                                                 (user-uid            user)
                                                 (user-gid            user)
                                                 (user-gecos          user)
                                                 (user-home-directory user)
                                                 (user-shell          user)))))

    ((vector "group" group-name)
     (let ((group (group-name->group group-name)))
       (displayln (format "~a:~a:~a:~a" (group-group-name group)
                                        (group-password   group)
                                        (group-gid        group)
                                        (string-join (group-members group) ",")))))

    (_ (error "Not enough arguments"))))


(module+ test
  (require rackunit)

  (check-equal? (user-uid (username->user "root")) 0)
  (check-equal? (user-username (uid->user 0)) "root")

  (check-equal? (group-gid (group-name->group "root")) 0)
  (check-equal? (group-group-name (gid->group 0)) "root"))
