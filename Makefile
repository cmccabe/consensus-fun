CFLAGS = -O2 -g -Wall -Wextra
LDFLAGS = -lpthread
test_2pc: test_2pc.o worker.o
