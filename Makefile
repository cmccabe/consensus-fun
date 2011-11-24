CFLAGS = -O2 -g -Wall -Wextra
LDFLAGS = -lpthread -lrt
test_2pc: test_2pc.o worker.o

clean:
	rm -f *.o test_2pc
