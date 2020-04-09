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
         (rename-in ffi/unsafe (-> -->))
         ffi/unsafe/define)


(provide
  (struct-out user)
  (struct-out group)

  (contract-out
    (username->user (-> string? user?))
    (uid->user      (-> exact-nonnegative-integer? user?))
    (name->group    (-> string? group?))
    (gid->group     (-> exact-nonnegative-integer? group?))))


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
    (define (build (built empty))
      (define group (ptr-ref ptr _string (length built)))
      (match group
        (#f          (reverse built))
        ((? string?) (build (cons group built)))))

    (build)))

; NOTE This is the GNU definition, which differs from BSD (e.g., macOS)
;      and, strictly, POSIX (which doesn't define pw_gecos)
(define-cstruct _passwd/gnu ((pw_name   _string)
                             (pw_passwd _string)
                             (pw_uid    _uid_t)
                             (pw_gid    _gid_t)
                             (pw_gecos  _string)
                             (pw_dir    _path)
                             (pw_shell  _path)))

; NOTE The group struct definition is consistent across libc implementations
(define-cstruct _group/gnu  ((gr_name   _string)
                             (gr_passwd _string)
                             (gr_gid    _gid_t)
                             (gr_mem    _string/list)))


;; User-facing structures
(struct user (username password uid gid gecos home shell))
(struct group (name password gid members))

(define passwd/gnu->user (compose (curry apply user) passwd/gnu->list))
(define group/gnu->group (compose (curry apply group) group/gnu->list))


;; libc Wrappers
(define-ffi-definer define-libc (ffi-lib #f))

(define (check-null fn)
  (lambda (arg) (or (fn arg) (error 'FFI "Failed [errno:~a]" (saved-errno)))))

(define-libc getpwnam
  (_fun #:save-errno 'posix _string --> _passwd/gnu-pointer/null)
  #:wrap check-null)

(define-libc getpwuid
  (_fun #:save-errno 'posix _uid_t  --> _passwd/gnu-pointer/null)
  #:wrap check-null)

(define-libc getgrnam
  (_fun #:save-errno 'posix _string --> _group/gnu-pointer/null)
  #:wrap check-null)

(define-libc getgrgid
  (_fun #:save-errno 'posix _gid_t  --> _group/gnu-pointer/null)
  #:wrap check-null)


;; User-facing functions
(define username->user (compose passwd/gnu->user getpwnam))
(define uid->user      (compose passwd/gnu->user getpwuid))
(define name->group    (compose group/gnu->group getgrnam))
(define gid->group     (compose group/gnu->group getgrgid))


(module+ main
  (require racket/string)

  (define (user->string user)
    (format "~a:~a:~a:~a:~a:~a:~a" (user-username user)
                                   (user-password user)
                                   (user-uid      user)
                                   (user-gid      user)
                                   (user-gecos    user)
                                   (user-home     user)
                                   (user-shell    user)))

  (define (group->string group)
    (format "~a:~a:~a:~a" (group-name     group)
                          (group-password group)
                          (group-gid      group)
                          (string-join (group-members group) ",")))

  (define numeric-string? (curry regexp-match? #rx"^(0|[1-9][0-9]*)$"))

  ; Very basic frontend; not a replacement for getent!
  (match (current-command-line-arguments)
    ((vector "passwd" uid) #:when (numeric-string? uid)
     (let ((user (uid->user (string->number uid))))
       (displayln (user->string user))))

    ((vector "passwd" username)
     (let ((user (username->user username)))
       (displayln (user->string user))))

    ((vector "group" gid) #:when (numeric-string? gid)
     (let ((group (gid->group (string->number gid))))
       (displayln (group->string group))))

    ((vector "group" group-name)
     (let ((group (name->group group-name)))
       (displayln (group->string group))))

    (_ (error "Invalid arguments"))))


(module+ test
  (require rackunit)

  (check-equal? (user-uid (username->user "root")) 0)
  (check-equal? (user-username (uid->user 0)) "root")

  (check-equal? (group-gid (name->group "root")) 0)
  (check-equal? (group-name (gid->group 0)) "root"))
