package org.sqlited.io;

public interface Protocol {

    byte VERSION = 0x01;

    // Command list
    // - stmt command
    byte CMD_CREATE_STMT  = 0x01;
    byte CMD_EXECUTE      = 0x02;
    byte CMD_FETCH_ROWS   = 0x03;
    byte CMD_CLOSE_STMT   = 0x04;
    // - tx command
    byte CMD_SET_TI       = 0x51; // set tx isolation
    byte CMD_SET_RO       = 0x52; // set readonly
    byte CMD_SET_AC       = 0x53; // set autocommit
    byte CMD_SET_SP       = 0x54; // set savepoint
    byte CMD_REL_SP       = 0x55; // release savepoint
    byte CMD_COMMIT       = 0x56;
    byte CMD_ROLLBACK     = 0x57; // rollback [savepoint]
    byte CMD_SET_HD       = 0x58; // set holdability

    // Results
    byte RESULT_OK  = 0;
    byte RESULT_ER  = 1;
    byte RESULT_SET = 2;

    // Types
    // - Object
    byte TYPE_OBJ_INT     = 101;  // Long
    byte TYPE_OBJ_REAL    = 102;  // Double
    byte TYPE_OBJ_TEXT    = 103;  // String
    byte TYPE_OBJ_BLOB    = 104;  // byte[]
    byte TYPE_OBJ_NULL    = 105;  // null
    // - Array
    byte TYPE_ARR_int     = 106;  // int[]
    byte TYPE_ARR_double  = 107;  // double[]
    byte TYPE_ARR_String  = 108;  // String[]
    byte TYPE_ARR_long    = 109;  // long[]
    byte TYPE_ARR_Object  = 110;  // Object[]: INT/REAL/TEXT/BLOB/NULL

}
