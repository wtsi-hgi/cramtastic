; Copyright (c) 2013 JP Verkamp
;
; Redistribution and use in source and binary forms, with or without
; modification, are permitted provided that the following conditions are
; met:
;
; 1. Redistributions of source code must retain the above copyright
;    notice, this list of conditions and the following disclaimer.
;
; 2. Redistributions in binary form must reproduce the above copyright
;    notice, this list of conditions and the following disclaimer in the
;    documentation and/or other materials provided with the
;    distribution.
;
; 3. The name of the author may not be used to endorse or promote
;    products derived from this software without specific prior written
;    permission.
;
; THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR
; IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
; DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
; INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
; (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
; SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
; HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
; STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
; IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
; POSSIBILITY OF SUCH DAMAGE.

#lang racket

(require file/gzip 
         file/gunzip)

(provide/contract 
 (with-gzip
   (->* ((-> any))
        (#:buffer-size (or/c false? exact-positive-integer?))
        any)))

(define (with-gzip thunk #:buffer-size [buffer-size #f])
  (define-values (pipe-from pipe-to) (make-pipe buffer-size))
  (dynamic-wind
   void
   (λ ()
     (define t (thread (λ () (gzip-through-ports pipe-from (current-output-port) #f (current-seconds)))))
     (parameterize ([current-output-port pipe-to])
       (thunk))
     (close-output-port pipe-to)
     (thread-wait t))
   (λ ()
     (unless (port-closed? pipe-to) (close-output-port pipe-to))
     (unless (port-closed? pipe-from) (close-input-port pipe-from))))
  (void))

(provide/contract 
 (with-gunzip 
   (->* ((-> any))
        (#:buffer-size (or/c false? exact-positive-integer?))
        any)))
                  
(define (with-gunzip thunk #:buffer-size [buffer-size #f])
  (define-values (pipe-from pipe-to) (make-pipe buffer-size))
  (dynamic-wind
   void
   (λ ()
     (thread 
      (λ ()
        (gunzip-through-ports (current-input-port) pipe-to)
        (close-output-port pipe-to)))
     (parameterize ([current-input-port pipe-from])
       (thunk)))
   (λ ()
     (unless (port-closed? pipe-to) (close-output-port pipe-to))
     (unless (port-closed? pipe-from) (close-input-port pipe-from)))))
