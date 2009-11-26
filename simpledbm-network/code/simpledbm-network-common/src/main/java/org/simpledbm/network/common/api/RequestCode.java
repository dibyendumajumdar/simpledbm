package org.simpledbm.network.common.api;

public interface RequestCode {
    int OPEN_SESSION = 1;
    int CLOSE_SESSION = 2;
    int START_TRANSACTION = 3;
    int COMMIT_TRANSACTION = 4;
    int ABORT_TRANSACTION = 5;
    int CREATE_TABLE = 6;
}
