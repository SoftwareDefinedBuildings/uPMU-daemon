all: sender.c
	gcc sender.c -g3 -o sender -Wall

clean:
	rm *~ *.pyc
