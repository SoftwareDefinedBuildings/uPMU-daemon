all: sender.c structs.c
	gcc -o sendd -g sender.c structs.c
