package org.springframework.batch.item.database;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.batch.item.sample.Foo;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

public class SingleKeyFooDao extends JdbcDaoSupport implements FooDao {

    @Override
	public Foo getFoo(Object key){

		RowMapper<Foo> fooMapper = new RowMapper<Foo>(){
            @Override
			public Foo mapRow(ResultSet rs, int rowNum) throws SQLException {
				Foo foo = new Foo();
				foo.setId(rs.getInt(1));
				foo.setName(rs.getString(2));
				foo.setValue(rs.getInt(3));
				return foo;
			}
		};

		return getJdbcTemplate().query("SELECT ID, NAME, VALUE from T_FOOS where ID = ?",
				new Object[] {key}, fooMapper).get(0);

	}
}
