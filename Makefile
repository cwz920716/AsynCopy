RM = rm -f
CC = gcc
RUN = ./a.out

# the build dependency:

# the build target executable:
ALL_LIBS = *.o

clean:
	$(RM) $(ALL_LIBS)

