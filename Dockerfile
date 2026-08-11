FROM gcr.io/distroless/static-debian13:nonroot

COPY out/bin/dispatcher /app

CMD ["/app", "serve"]