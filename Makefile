all: sender.c
	gcc sender.c -o sender

clean:
	rm *~ *.pyc
