#lang racket

(require racket/match)
(require net/http-client)
(require json)

(define *bnw-host-name* "bnw.im")
(define *stats-filename* "bnwstats.dot")
(define *msgcount-criteria* 10)
(define *comments-criteria* 20)
(define *lastmsg-criteria* 15768000) ;; half year should be enough
(define *default-fontsize* 30)
(define *max-font-factor* (* *default-fontsize* 2))
(define *threads-num* 20)

(struct global-ctx (userhash))
(struct userinfo (name subscribers subscriptions messages-count
                       regdate comments-count lastmessage-time) #:transparent)

(define *bnw-ctx* (global-ctx (make-hash)))

(define/contract (make-request ctx uri)
  (-> global-ctx string? jsexpr?)
  (match-define-values (stat srv-info inpp)
                       (http-sendrecv *bnw-host-name* uri))
  (read-json inpp))

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

(define/contract (user-known? ctx name)
  (-> global-ctx? string? boolean?)
  (hash-has-key? (global-ctx-userhash ctx) name))

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
  (hash-ref userhash name))

(define/contract (get-user-info/memoize ctx name)
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
  (when (user-meets-criteria? alice-uinfo)
    (match-let 
        ([(userinfo name subscribers subscriptions messages-count regdate comments-count lastmessage-time)
          alice-uinfo])

      (eprintf "~a: building reader graph\n" alice)
      (for ([bob (in-list subscribers)])
        (when (and #t
                   (user-known? ctx bob)
                   (user-meets-criteria? (get-user-info ctx bob)))
          (printf "\"~a\" -> \"~a\"\n" bob alice))))))

(define/contract (output-nodes-info ctx)
  (-> global-ctx? void?)
  (define max-subs (get-max-subscriptions ctx))

  (for ([(name ui) (in-hash (global-ctx-userhash ctx))]
        #:when (user-meets-criteria? ui))
    (define subs-len (length (userinfo-subscribers ui)))
    (printf "\"~a\" [fontsize=~a]\n" name
            (round (+ *default-fontsize* (* *max-font-factor*
                                            (/ subs-len max-subs)))))))

(define/contract (output-dot ctx)
  (-> global-ctx? void?)
  (define userlist (get-all-usernames ctx))
  (printf "digraph {\n")
  (printf "graph [overlap = scale, outputorder = edgesfirst]\n")
  (printf "node [style = filled]\n")
  (for ([alice (in-list userlist)])
    (output-dot-for-user ctx alice))
  (output-nodes-info ctx)
  (printf "}"))

(define/contract (get-max-subscriptions ctx)
  (-> global-ctx? integer?)
  (for/fold ([max-subs 0]) ([(name ui) (in-hash (global-ctx-userhash ctx))])
    (define sc (length (userinfo-subscribers ui)))
    (if (> sc max-subs)
        sc
        max-subs)))

(define/contract (split-list-to-n lst n)
  (-> list? integer? list?)
  
  (define slice-sz (floor (/ (length lst) n)))
  (define (do-split remlst resn)
    (if (eq? resn 1)
        (list remlst)
        (append (list (take remlst slice-sz))
                (do-split (drop remlst slice-sz) (sub1 resn)))))

  (do-split lst n))

(define/contract (init-userhash ctx)
  (-> global-ctx? any)
  (eprintf "Retrieving user info.\n")
  (define userlist (get-all-usernames ctx))
  (define ul-len (length userlist))

  (define worksize (floor (/ ul-len *threads-num*)))

  (define thread-list
    (for/list ([workset (in-list (split-list-to-n userlist *threads-num*))])
      (thread
       (lambda ()
         (for ([u (in-list workset)])
           (get-user-info/memoize ctx u))))))

  (for ([tr (in-list thread-list)])
    (thread-wait tr)))

(define/contract (generate-dot-file ctx)
  (-> global-ctx? void?)
  (with-output-to-file *stats-filename*
    (λ () (output-dot ctx))
    #:mode 'text
    #:exists 'truncate))

(time
 (init-userhash *bnw-ctx*))
(time
 (generate-dot-file *bnw-ctx*))

