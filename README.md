# go-webhook-challenge

________


### Preface

To begin with, I was happy to being given the freedom to use any approach and technology I could see fit for this task.
It allowed for a straight-to-the-point approach to what I thought about for a solution, and getting it running quicker.

### Concept

The app (`dispatcher`) is responsible for ingesting HTTP POST requests in the form of jobs which are to be processed 
individually. In this case, the processing is simply to send its payload to the respective destination URI that is 
provided.

### Implementation details

This is a state machine for (potentially long-processing) jobs and could be integrated in a microservices approach by 
leveraging a messaging system with an async API (e.g. Nats, Solace, RabbitMQ), which would handle the tasks queue from 
one app (ingester) to another (processor). The message queue takes care of a lot of the responsibilities in this context
given that it could be configured for its retries and dead-letter-queue for failed items.

Instead, I opted for something different. I chose to use a PostgreSQL instance to create the same solution using a 
simplified outbox pattern. This pattern separates the act of reading a message or request and processing it. It applies
to apps bound by async APIs (for reliability), but also as a state machine processor with persistence. This approach 
will ensure that all incoming requests are recorded and separately (in a goroutine) a poller will find new jobs to work 
on. Having a persistence layer in this setup ensures that, if anything fails while processing each job, that we can 
retry it given the last state in the database (thus, survives issues with networking, database connections, or app 
crashes). 

To allow concurrent processing of jobs, we can use the `FOR UPDATE SKIP LOCKED` clause, and work with a single job 
alongside an open transaction in Postgres. This disallows overlapping of rows when fetching them concurrently.

### Technology

- **Transport**: gRPC + gRPC-gateway (HTTP 1.1)
- **Persistence**: PostgreSQL
- **Infra**: Docker / Docker compose

### Approach

#### Transport

I started with my usual approach to building a new web app, API-first. I set up the protobuf schema for the dispatch 
service that I needed to expose and wrote its RPC and request / response messages. This is usually a simple way of 
visualizing the API I will consume and using `buf`, I can generate all the transport layer logic (with drop-in
packages), as well as keeping swagger docs on the API.

`buf` is an amazing tool for working with protobuf and gRPC -- for this and the `validate.proto` file, the `buf` setup
files, the `httpserver` and `grpcserver`, I reused the logic 
[from another personal project I've worked on in the past](https://github.com/zalgonoise/x/tree/master/authz)

Another plus to this set of tools is that the `validate` proto file allows extensions (as seen in the message), where
we set our validation rules in the message descriptors directly. This means better readability on validation, 
documentation for it, as well as a simple `request.ValidateAll()` call in the service layer.

#### Service

From here, I sketched out the service layer with the `dispatcher` and the `poller` packages, without any logic yet, 
besides the symbols I needed (types and their methods, and the repository interface method definitions that I would 
consume). Of course these suffered some changes later on but the initial draft allowed me to move to the persistence 
layer.

#### Persistence

I found it funny that having designed schemas and database queries for scenarios like this before (for Postgres in 
particular), I had no examples from my personal projects. So I did some research on how to use golang-migrate again, 
which is simple enough, and I remembered that I needed the `FOR UPDATE SKIP LOCKED` clause on a 
`SET ... WHERE ... (SELECT...)` query. I used **Gemini Code Assist** to build my poller query. Curious that you 
mentioned which changes I've made to AI-generated suggestions, since the generated query involved creating a subtable to
select the most recent N results. Since this felt too cumbersome (I didn't analyze the queries -- with more time 
invested it would be the ideal approach), i reverted it to a nested query on the `WHERE` clause, with a `LIMIT 1` clause
(since I would pick up 1 job at a time, on N workers).

#### Poller logic

Having my `repository` package ready, I moved over to the poller first, knowing that I would need to break it down in 
two parts: the loop that starts off from the goroutine in main.go, and processing tasks on each tick. The polling 
logic is very simple since we just need to listen to a ticker and to a context.Done signal.

For processing tasks, it eventually was decoupled into two separate methods, but the idea for concurrency here was 
simple: use the `errgroup` package and just set a limit from there. All the semaphore logic is already included and 
well written by the Go-team so it's a great (and short) option for limits on goroutines like these. Another detail here,
as analyzing the logic in this package, was that Gemini mentioned that the loop in `HandleBatch` could lead to overlaps
if each task took longer than a cycle's time. Its solution involved another timeout value and another
context with timeout, however I opted for a simpler approach here, by just taking each frequency duration minus one 
second, and using this as the context timeout value.

As for handling each job, I wrote the HTTP client logic to send out the HTTP request and handle any failures along the 
way. I had not made the transaction (`JobTx`) changes here yet.

#### Dispatcher logic

Writing the dispatcher made me go back to the protobuf specification as I was considering a `bytes` field for the 
payload, however this wasn't practical (or correct) to use in an HTTP request; and implementing it made me realize I 
needed to use a `google.protobuf.Struct` type instead.

I used Gemini to create a quick `generateSignature` for the HMAC signature, and I was OK with its suggestion (it's very 
simple anyway) so I didn't change anything in it.

#### Wiring together

From here, I could start connecting the modules together in `main.go` more easily. While I had a sketch already up until
I initialized the services, I needed to include a config package to manage configuration. I used `cloudresty/go-env` for
the first time and it was quite nice for the `.env` file support, but any package is fine. (We only load env once on 
startup, and struct tags are bound to be used with reflection, the performance impact is minor.)

For the CLI logic, [I used my own package](https://github.com/zalgonoise/x/tree/master/cli), since it feels familiar but
honestly it's very short and simple. For small apps like these, it's fine.

Connecting all the modules together I added the drop-in `httpserver`, `grpcserver` and `log` from
[my *authz* project as well](https://github.com/zalgonoise/x/tree/master/authz), except for the metrics and tracing 
logic (which I would love to have included, alongside a nice Grafana dashboard with metrics, traces and logs connected),
as I felt I was taking too long with the task already.

#### Making it work

I've tested its functionality manually with `curl` HTTP requests and fixed some of the issues that weren't very 
obvious to the eye nor to AI (hah!). Namely the transactions bound to each fetched job in the poller. Not binding
a transaction (and calling Commit / Rollback on it) with the update-lock was causing jobs not to change status.

I took the time to write two helper servers (accepter and rejecter), as well as a demo-run script that shows a response
of the accepter and rejecter servers, a successfully dispatched job a failed job, and a series of 20 jobs dispatched 
very close to another. The jobs can be found on the postgres database in the docker compose stack, in the 
webhook_jobs table, as well as the logs output from the app.

I used Gemini for writing tests for some of these modules. I was happy to see `testify` being used and I was mostly 
satisfied with the results with minor corrections (mostly on formatting or structure, but the tests were actually 
acceptable). Generated tests for modules with mocks required some changes (e.g. `dispatcher` did not calculate the HMAC
signatures correctly, which is fine and easily fixable).

#### Missing details

I would probably invest more time on observability (metrics and traces) which are missing, as well as reliability for 
certain weak-points in this implementation (poller, and possibly analyzing how could I make the SQL queries better or 
faster). Testing would need a big boost as well, including proper integration tests and end-to-end tests.

#### Final notes

Thank you for reading my submission and reading my code! It was a pleasure to write it and I appreciate the opportunity!


#### Run it yourself!

1. To build the app, you can run `make build`. This prepares both the binaries and respective docker containers.
2. If you wish to run the compose stack, run `make compose`
3. On a separate terminal, run the demo-run script with `make demo`