package pl.kielce.tu.cassandra.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.ArithmeticRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Update;

import com.datastax.oss.driver.internal.querybuilder.term.ArithmeticTerm;
import pl.kielce.tu.cassandra.simple.SimpleManager;

import java.util.Random;
import java.util.Scanner;

public class StworzJednostke {

    public void stworzTabele(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable("jednostka")
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("Nazwa_jednostki", DataTypes.TEXT)
                .withColumn("Ilosc_zgloszen", DataTypes.INT);
        session.execute(createTable.build());
    }

    public void stworz(CqlSession session, Long id, String nazwa, int ilZgloszen) {
        Insert insert = QueryBuilder.insertInto("Straz_Pozarna", "jednostka")
                .value("id", QueryBuilder.raw(String.valueOf(id)))
                .value("Nazwa_jednostki", QueryBuilder.raw("'" + nazwa + "'"))
                .value("Ilosc_zgloszen", QueryBuilder.raw(String.valueOf(ilZgloszen)));
        session.execute(insert.build());
        System.out.println("PUT " + id + " => " + " nazwa: '" + nazwa + "' ilosc zgloszen: " +ilZgloszen);
    }
}
