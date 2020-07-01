package pl.kielce.tu.cassandra.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.update.Update;


import pl.kielce.tu.cassandra.simple.SimpleManager;

import java.util.Random;
import java.util.Scanner;



public class StudentsTableBuilderManager extends SimpleManager {
	final private static Random r = new Random(System.currentTimeMillis());

	public StudentsTableBuilderManager(CqlSession session) {
		super(session);
	}

	public void dodaj() {
		Long key1 = (long) Math.abs(r.nextInt());
		byte key11 = (byte) Math.abs(r.nextInt(60));
		StworzJednostke jednostka = new StworzJednostke();

		jednostka.stworz(session, key1, "Myslowice", key11 + 30);

		Long key2 = (long) Math.abs(r.nextInt());
		byte key12 = (byte) Math.abs(r.nextInt(60));
		jednostka.stworz(session, key2, "Wschowa", key12+ 30);


		Long key3 = (long) Math.abs(r.nextInt());
		byte key13 = (byte) Math.abs(r.nextInt(60));
		jednostka.stworz(session, key3, "Rabka", key13+ 30);


		Long key4 = (long) Math.abs(r.nextInt());
		byte key14 = (byte) Math.abs(r.nextInt(60));
		jednostka.stworz(session, key4, "Lubsko", key14+ 30);


		Long key5 = (long) Math.abs(r.nextInt());
		byte key15 = (byte) Math.abs(r.nextInt(60));
		jednostka.stworz(session, key5, "Wieliczka", key15+ 30);


		Long key6 = (long) Math.abs(r.nextInt());
		byte key16 = (byte) Math.abs(r.nextInt(60));
		jednostka.stworz(session, key6, "Zgierz", key16+ 30);
	}

	public void deleteFromTable() {
		Scanner in = new Scanner(System.in);
		System.out.println("Podaj id wybranej jednostki strazy pozarnej aby ją usunąć");
		String wybor = "";
		wybor = in.nextLine();

		Delete delete = QueryBuilder.deleteFrom("jednostka").whereColumn("id").isEqualTo(literal(Integer.parseInt(wybor)));
		session.execute(delete.build());
	}

	public void pobierzWszystko() {
		Select query = QueryBuilder.selectFrom("jednostka").all();
		SimpleStatement statement = query.build();
		selectFromTable(statement);
	}

	public void pobierzPoNazwie() {
		Scanner in = new Scanner(System.in);
		System.out.println("Podaj nazwę jednostki strazy pozarnej którą chcesz wyszukać");
		String wybor = "";
		wybor = in.nextLine();

		Select query = QueryBuilder.selectFrom("jednostka").all().whereColumn("Nazwa_jednostki").isEqualTo(literal(wybor)).allowFiltering();
		SimpleStatement simpleStatement = query.build();
		selectFromTable(simpleStatement);
	}



	public void pobierzZWarunkami() {
		Select query = QueryBuilder.selectFrom("jednostka").all().allowFiltering()
				.whereColumn("Ilosc_zgloszen").isGreaterThan(literal(70))
				.whereColumn("Ilosc_zgloszen").isLessThan(literal(85))
				.whereColumn("Nazwa_jednostki").isEqualTo(QueryBuilder.literal("Rabka"));
		SimpleStatement simpleStatement = query.build();
		selectFromTable(simpleStatement);
	}


	public void przetworz() {

		Scanner in = new Scanner(System.in);
		System.out.println("Podaj id wybranej jednostki strazy pozarnej aby zmienic ilosc zlecen");
		String wybor = "";
		wybor = in.nextLine();

		Update update1 = QueryBuilder.update("jednostka")
				.setColumn("Ilosc_zgloszen", QueryBuilder.literal(10))
				.whereColumn("id").isEqualTo(literal(Integer.parseInt(wybor)));


		session.execute(update1.build());
	}


	public void selectFromTable(SimpleStatement simpleStatement) {
		ResultSet resultSet = session.execute(simpleStatement);
		for (Row row : resultSet) {
			System.out.print("Straż Pożarna: ");
			System.out.print("id=" + row.getInt("id") + ", ");
			System.out.print("nazwa=" + row.getString("Nazwa_jednostki") + ", ");
			System.out.print("ilość zgłoszeń=" + row.getInt("Ilosc_zgloszen"));
			System.out.println();
		}
		System.out.println("Statement \"" + simpleStatement.getQuery() + "\" executed successfully");
	}

	public void usunwszystko() {
		Drop drop = SchemaBuilder.dropTable("jednostka");
		executeSimpleStatement(drop.build());

		StworzJednostke jednostka = new StworzJednostke();
		jednostka.stworzTabele(session);
	}

	public void dropTable() {
		Drop drop = SchemaBuilder.dropTable("jednostka");
		executeSimpleStatement(drop.build());
	}
}
