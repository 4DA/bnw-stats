#lang racket
(require racket/match)
(require net/http-client)
(require json)

(define *bnw-host-name* "bnw.im")
(define *stats-filename* "/home/dc/bnwstats.dot")
(define *msgcount-criteria* 10)
(define *comments-criteria* 20)
(define *lastmsg-criteria* 15768000) ;; half year should be enough

(struct global-ctx (userhash))
(struct userinfo (name subscribers subscriptions messages-count
                       regdate comments-count lastmessage-time) #:transparent)

(define *bnw-ctx* (global-ctx (make-hash)))

(define/contract (make-request ctx uri)
  (-> global-ctx string? jsexpr?)
  (match-define-values (stat srv-info inpp)
                       (http-sendrecv *bnw-host-name* uri))

  (define jsl (read-json inpp))
  jsl)

(define/contract (get-users-by-page-num ctx page-num)
  (-> global-ctx? integer? list?)
  (define jsl (make-request ctx (format "/api/userlist?page=~a" page-num)))
  
  (when (hash-ref jsl 'ok)
    (define uh (hash-ref jsl 'users))
    (if (eq? uh '()) '()        
      (for/list ([entry (in-list uh)])
        (hash-ref entry 'name)))))

(define/contract (get-all-usernames ctx)
  (-> global-ctx? list?)
  (for/fold ([rsl '()]) ([page-num (in-naturals 0)])
    (define userlist (get-users-by-page-num ctx page-num))
    #:break (or (void? userlist) (eq? userlist '()))
    (append rsl userlist)))

(define/contract (get-user-lastmessage-time ctx name)
  (-> global-ctx? string? (or/c real? #f))
  (define jsl (make-request ctx (format "/api/show?user=~a" name)))
  (define messages (hash-ref jsl 'messages))
  
  (if (empty? messages) #f
      (hash-ref (last messages)
                'date)))

(define/contract (user-actually-exists? ctx name)
  (-> global-ctx? string? boolean?)
  (define jsl (make-request ctx (format "/api/userinfo?user=~a" name)))
  (eq? #t (hash-ref jsl 'ok)))

(define/contract (request-user-info ctx name)
  (-> global-ctx? string? userinfo?)
  (define jsl (make-request ctx (format "/api/userinfo?user=~a" name)))

  (if (eq? #t (hash-ref jsl 'ok))
    (userinfo name
              (hash-ref jsl 'subscribers_all)
              (hash-ref jsl 'subscriptions_all)
              (hash-ref jsl 'messages_count)
              (hash-ref jsl 'regdate)
              (hash-ref jsl 'comments_count)
              (get-user-lastmessage-time ctx name))
  (error (format "Can not request user info: ~a\nResponse: ~a" name jsl))))

(define/contract (get-user-info ctx name)
  (-> global-ctx? string? userinfo?)

  (define userhash (global-ctx-userhash ctx))
  (if (hash-has-key? userhash name)
      (hash-ref userhash name)
      (let ([uinfo (request-user-info ctx name)])
        (hash-set! userhash name uinfo)
        uinfo)))

(define/contract (user-meets-criteria? bob)
  (-> userinfo? boolean?)
  (match-let ([(userinfo _ bob-readers bob-subs bob-msgcount _ bob-comments bob-lastmsg ) bob])
    (and (> bob-msgcount *msgcount-criteria*)
         (> bob-comments *comments-criteria*)
         (< (- (current-seconds) bob-lastmsg) *lastmsg-criteria* ))))

(define/contract (output-dot-for-user ctx alice)
  (-> global-ctx? string? void?)

  (define alice-uinfo (get-user-info ctx alice))
  (if (user-meets-criteria? alice-uinfo)
      (match-let 
          ([(userinfo name subscribers subscriptions messages-count regdate comments-count lastmessage-time)
            alice-uinfo])

        (eprintf "~a: building reader graph\n" alice)
        (for ([bob (in-list subscribers)])
          (when (and (user-actually-exists? ctx bob)
                     (user-meets-criteria? (get-user-info ctx bob)))
            (printf "\"~a\" -> \"~a\"\n" bob alice))))

      (eprintf "~a: doesn't meet criteria\n" alice)))

(define/contract (output-dot ctx)
  (-> global-ctx? void?)
  (define userlist (get-all-usernames ctx))
  (printf "digraph {\n")
  (printf "graph [overlap = scale, outputorder = edgesfirst]\n")
  (printf "node [style = filled]\n")
  (for ([alice (in-list userlist)])
    (output-dot-for-user ctx alice))
  (printf "}"))

(define (generate-dot-file ctx)
  (with-output-to-file *stats-filename*
    (λ () (output-dot ctx))
    #:mode 'text
    #:exists 'truncate))

 (generate-dot-file *bnw-ctx*)



