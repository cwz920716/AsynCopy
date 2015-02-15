RM = rm -rf
CC = gcc -pthread -Wall
RUN = ./a.out

# the build dependency:
ALL_SRC = *.c
ALL_HEADERS = *.h

# the build target executable:
ALL_LIBS = *.o 

all: $(ALL_HEADERS) $(ALL_SRC)
	$(CC) -o acp $(ALL_SRC)

run:
	./acp ../../test/linux-2.6.32.64 ../../garbage/1

clean:
	$(RM) $(ALL_LIBS) *.gch acp
	$(RM) ../../garbage/*

