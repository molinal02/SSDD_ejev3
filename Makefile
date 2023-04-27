CC=gcc
CFLAGS=-g -Wall -I/usr/include/tirpc
LDLIBS += -lnsl -lpthread -ldl -ltirpc
OBJS_SERVER=stub_servidor.o libservidor.a
OBJS_CLIENT=cliente.o 
OBJS= $(OBJS_SERVER) $(OBJS_CLIENT) libclaves.so servidor.o ejev3_xdr.o

all: libclaves.so $(OBJS_SERVER) $(OBJS_CLIENT)
	$(CC) $(CFLAGS) -o servidor_ejev3 $(OBJS_SERVER) $(LDLIBS)
	$(CC) $(CFLAGS) -o cliente_ejev3 $(OBJS_CLIENT) $(LDLIBS) -L. -lclaves

libservidor.a: servidor.o ejev3_xdr.o
	ar rc libservidor.a servidor.o ejev3_xdr.o

libclaves.so: claves.c stub_cliente.c ejev3_xdr.c
	$(CC) -g -Wall -shared -fPIC -o libclaves.so claves.c stub_cliente.c ejev3_xdr.c

%.o: %.c
	$(CC) -g -Wall -c $< -o $@

clean:
	rm -f $(OBJS) servidor_ejev3 cliente_ejev3