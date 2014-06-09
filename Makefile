all: sender.c
	gcc sender.c -g3 -o sender -Wall

crosscompile: sender.c
	arm-none-linux-gnueabi-gcc -o sender-arm sender.c

clean:
	rm *~ *.pyc
