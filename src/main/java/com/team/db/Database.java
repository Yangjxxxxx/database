package com.team.db;

import java.util.List;

public interface Database {

    List<String> query(String param);

    int matchesCount(String param);

}
