FROM golang:1.19.2-alpine

WORKDIR /src

COPY . .

# Install Go Dependencies
RUN go mod download

# Build the application
RUN make clean
RUN make

# TODO: Probably need to expose some ports here...

# Run the application
CMD ["./app", "-f", "output.log"]