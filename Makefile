all: sender.c
	gcc sender.c -o sender

crosscompile: sender.c
	arm-none-linux-gnueabi-gcc -o sender-arm sender.c

clean:
	rm *~ *.pyc
