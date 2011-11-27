CFLAGS = -O2 -g -Wall -Wextra
LDFLAGS = -lpthread -lrt
all: test_2pc test_paxos
test_2pc: test_2pc.o util.o worker.o limits.h tree.h util.h worker.h
test_paxos: test_paxos.o util.o worker.o limits.h tree.h util.h worker.h

clean:
	rm -f *.o test_2pc
