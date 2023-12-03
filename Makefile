# Makefile

EXECS := app word_count

all: $(EXECS)

app:
	go build -o app ./cmd/app

word_count:
	go build -o bin/maple_word_count ./cmd/maples/maple_word_count
	go build -o bin/juice_word_count ./cmd/juices/juice_word_count

clean:
	rm -rf bin
	rm -f app

.PHONY: all clean $(EXECS)
