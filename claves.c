#include <stdio.h>
#include <stdlib.h>
#include <mqueue.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <stdarg.h>

#include "claves.h"
#include "ejev3.h"


// Inicializa el servicio de almacenaje de tuplas <clave-valor1-valor2-valor3>
int init() {

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = init_tuples_1(&result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;

}


// Insercion del elemento <clave-valor1-valor2-valor3>
int set_value(int key, char* value1, int value2, double value3) {

    // Revisar si value1 de tupla cumple con el requesito de maximo 255 caracteres (excluido '\0')
    if (strlen(value1) > 256 || (strlen(value1) == 256 && value1[255] != '\0')){
        perror("[CLIENTE][ERROR] La longitud maxima de value1 es 256 caracteres, siendo el nº 256 exclusivo para \'\\0\'\n");
        return -1;
    }

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }
    
    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = set_value_tuples_1(key, value1, value2, value3, &result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;
}


// Obtencion de los valores asociados a la clave proporcionada
int get_value(int key,char* value1, int* value2, double* value3) {
    

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    t_service* result;

    result = (t_service*) malloc(sizeof(t_service));

    retval = get_value_tuples_1(key, result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        free(result);
        clnt_destroy(clnt);
        return -1;
    }

    if (result->status == 0) {
        strcpy(value1, result->content.value1);
        *value2 = result->content.value2;
        *value3 = result->content.value3;
    }

    int op_status = result->status;

    free(result);
    clnt_destroy(clnt);
    return op_status;
}


// Modificacion de los valores asociados a la clave proporcionada
int modify_value(int key, char* value1, int value2, double value3) {

    // Revisar si value1 de tupla cumple con el requesito de maximo 255 caracteres (excluido '\0')
    if (strlen(value1) > 256 || (strlen(value1) == 256 && value1[255] != '\0')){
        perror("[CLIENTE][ERROR] La longitud maxima de value1 es 256 caracteres, siendo el nº 256 exclusivo para \'\\0\'\n");
        return -1;
    }

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = modify_value_tuples_1(key, value1, value2, value3, &result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;
}
    
// Eliminacion de la tupla asociada a la clave proporcionada
int delete_key(int key) {


    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = delete_key_tuples_1(key, &result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;
}


// Comprobacion de la existencia de algun elemento asociado a la clave proporcionada
int exist(int key) {

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = exist_tuples_1(key, &result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;
}


// Creacion e insercion de un nuevo elemento con la segunda clave proporcionada, copiando los valores de la primera
int copy_key(int key1, int key2) {

    char* host = getenv("IP_TUPLAS");
    if (host == NULL) {
        perror("[CLIENTE][ERROR] No se ha definido la variable de entorno IP_TUPLAS\n");
        return -1;
    }

    CLIENT* clnt = clnt_create (host, TUPLAS, TUPLASVER, "tcp");
	if (clnt == NULL) {
		clnt_pcreateerror(host);
		return -1;
	}

    enum clnt_stat retval;
    int result;

    retval = copy_key_tuples_1(key1, key2, &result, clnt);

    if (retval != RPC_SUCCESS) {
        clnt_perror (clnt, "[CLIENTE][ERROR] La conexion no fue exitosa\n");
        clnt_destroy(clnt);
        return -1;
    }

    clnt_destroy(clnt);
    return result;
}