# Makefile

EXECS := app word_count demo sql

all: $(EXECS)

app:
	go build -o app ./cmd/app

word_count:
	go build -o bin/maple_word_count ./cmd/maples/maple_word_count
	go build -o bin/juice_word_count ./cmd/juices/juice_word_count

demo:
	go build -o bin/maple_demo_phase1 ./cmd/maples/maple_demo_phase1
	go build -o bin/maple_demo_phase2 ./cmd/maples/maple_demo_phase2
	go build -o bin/juice_demo_phase1 ./cmd/juices/juice_demo_phase1
	go build -o bin/juice_demo_phase2 ./cmd/juices/juice_demo_phase2

sql:
	go build -o bin/maple_SQL_filter ./cmd/maples/maple_SQL_filter
	go build -o bin/juice_SQL_filter ./cmd/juices/juice_SQL_filter

clean:
	rm -rf bin
	rm -f app

.PHONY: all clean $(EXECS)
