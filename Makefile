RM = rm -f
CC = gcc -Wall
RUN = ./a.out

# the build dependency:
ALL_SRC = *.c
ALL_HEADERS = *.h

# the build target executable:
ALL_LIBS = *.o 

all: $(ALL_HEADERS) $(ALL_SRC)
	$(CC) -o acp $(ALL_SRC)

clean:
	$(RM) $(ALL_LIBS) *.gch acp

