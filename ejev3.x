/* Estructura tupla */
struct tuple{
    int key;
    string value1<256>;
    int value2;
    double value3;
};

typedef tuple t_tuple;

/* Estructura para respuesta del servidor a cliente */
struct service{
    int status;
    t_tuple content;
};

typedef service t_service;

program TUPLAS {
    version TUPLASVER {
        int init_tuples() = 1;
        int set_value_tuples(int key, string value1<256>, int value2, double value3) = 2;
        t_service get_value_tuples(int key) = 3;
        int modify_value_tuples(int key, string value1<256>, int value2, double value3) = 4;
        int delete_key_tuples(int key) = 5;
        int exist_tuples(int key) = 6;
        int copy_key_tuples(int key1, int key2) = 7;
    } = 1;
} = 99;