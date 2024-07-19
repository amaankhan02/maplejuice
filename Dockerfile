FROM golang:1.21-alpine

# Create a working directory
WORKDIR /src

RUN apk add --no-cache make

# Install Go Dependencies
COPY go.mod ./
RUN go mod download

# Copy relevant source code
COPY cmd/ ./cmd/
COPY internal/ ./internal
COPY Makefile ./

# Build the application
RUN make clean
RUN make app

# Run the application
CMD ["./app", "-f", "output.log"]