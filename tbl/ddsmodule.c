#include <Python.h>

#include "scan.h"

static PyObject *my_scan_init(PyObject *self, PyObject *args) {
    struct scan_ctx *scan;
    PyObject *cobj;

    scan = scan_init();
    if (!scan)
        return PyErr_NoMemory();
    cobj = PyCObject_FromVoidPtr(scan, (void (*)(void *))scan_free);
    if (!cobj)
        scan_free(scan);
    return cobj;
}

static PyObject *my_scan_set_fd(PyObject *self, PyObject *args) {
    struct scan_ctx *scan;
    PyObject *cobj;
    int fd;

    if (!PyArg_ParseTuple(args, "O!i", &PyCObject_Type, &cobj, &fd))
        return NULL;
    scan = PyCObject_AsVoidPtr(cobj);
    
    scan_set_fd(scan, fd);

    Py_RETURN_NONE;
}

static PyObject *my_scan_set_aio(PyObject *self, PyObject *args) {
    struct scan_ctx *scan;
    PyObject *cobj;

    if (!PyArg_ParseTuple(args, "O!", &PyCObject_Type, &cobj))
        return NULL;
    scan = PyCObject_AsVoidPtr(cobj);

    scan_set_aio(scan);

    Py_RETURN_NONE;
}

static PyObject *my_scan_begin(PyObject *self, PyObject *args) {
    struct scan_ctx *scan;
    PyObject *cobj;

    if (!PyArg_ParseTuple(args, "O!", &PyCObject_Type, &cobj))
        return NULL;
    scan = PyCObject_AsVoidPtr(cobj);

    return PyInt_FromLong(scan_begin(scan));
}

static PyObject *my_scan_read_chunk(PyObject *self, PyObject *args) {
    struct scan_ctx *scan;
    PyObject *cobj, *result_data[2], *final_result;
    int result;
    struct scan_chunk_data scan_data[2];

    if (!PyArg_ParseTuple(args, "O!", &PyCObject_Type, &cobj))
        return NULL;
    scan = PyCObject_AsVoidPtr(cobj);

    result = scan_read_chunk(scan, scan_data);

    if (result & SCAN_CHUNK_FOUND) {
        result_data[0] = PyString_FromStringAndSize((char *)scan_data[0].buf,
                                                    scan_data[0].size);
        if (!result_data[0])
            return NULL;

        result_data[1] = PyString_FromStringAndSize((char *)scan_data[1].buf,
                                                    scan_data[1].size);
        if (!result_data[1]) {
            Py_DECREF(result_data[0]);
            return NULL;
        }

        PyString_ConcatAndDel(&result_data[0], result_data[1]);
    } else {
        Py_INCREF(Py_None);
        result_data[0] = Py_None;
    }

    final_result = Py_BuildValue("iN", result, result_data[0]);
    if (!final_result)
        Py_DECREF(result_data[0]);
    return final_result;
}

static PyMethodDef dds_methods[] = {
    { "init", my_scan_init, METH_VARARGS, "scan_init" },
    { "set_fd", my_scan_set_fd, METH_VARARGS, "scan_set_fd" },
    { "set_aio", my_scan_set_aio, METH_VARARGS, "scan_set_aio" },
    { "begin", my_scan_begin, METH_VARARGS, "scan_begin" },
    { "read_chunk", my_scan_read_chunk, METH_VARARGS, "scan_read_chunk" },
    { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC init_dds(void) {
    PyObject *m;
    m = Py_InitModule("_dds", dds_methods);
    if (!m)
        return;
    (void)PyModule_AddIntMacro(m, SCAN_CHUNK_FOUND);
    (void)PyModule_AddIntMacro(m, SCAN_CHUNK_LAST);
}

/* vim: set ts=8 sts=4 sw=4 cindent : */
