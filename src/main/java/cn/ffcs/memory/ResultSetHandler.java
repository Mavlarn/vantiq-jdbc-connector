package cn.ffcs.memory;


import java.sql.ResultSet;


public interface ResultSetHandler<T> {
	T handle(ResultSet rs) ;
}
