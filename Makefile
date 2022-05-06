PROG=pg_happy
LDFLAGS=-ldflags="-s -w"
SRCS=$(shell find . -name '*.go' -type f)

all: $(PROG)

$(PROG): $(SRCS)
	CGO_ENABLED=0 go build $(LDFLAGS) .

install: $(PROG)
	CGO_ENABLED=0 go install $(LDFLAGS) .

clean:
	-rm $(PROG)

.PHONY: all install clean
