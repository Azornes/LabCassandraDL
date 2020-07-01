package pl.kielce.tu.cassandra.builder;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Scanner;

public class TestCassandraBuilder {
	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "Straz_Pozarna");
			keyspaceManager.dropKeyspace();
			keyspaceManager.selectKeyspaces();
			keyspaceManager.createKeyspace();
			keyspaceManager.useKeyspace();

			StudentsTableBuilderManager operacja = new StudentsTableBuilderManager(session);
			StworzJednostke jednostka = new StworzJednostke();
			jednostka.stworzTabele(session);

			Scanner in = new Scanner(System.in);
			String wybor = "";

			while (wybor != "0") {
				System.out.println("----------------------------------------------- \n" +
						"Każda cyfra odpowiada następującej operacji: \n" +
						"0 - wychodzi z programu \n" +
						"1 - zapisz (dodaje dane)\n" +
						"2 - kasuj wybrane (usuwa wybrana straz pozarna ) \n" +
						"3 - kasuj (usuwa wszystkie dane)\n" +
						"4 - pobierz (wyswietla po nazwie) \n" +
						"5 - pobierz (wyswietla jednostki strazy pozarnej w Rabce tylko te które zrealizowaly zlecenia od 70 do 85 zlecen)\n" +
						"6 - pobierz (wyswietla wszystko)\n" +
						"7 - przetwórz (dodatkowe 10 zgłoszeń do wybranej jednostki straży pożarnej)\n" +
						"----------------------------------------------- \n"
				);

				wybor = in.nextLine();

				switch (wybor) {
					case "1":
						operacja.dodaj();
						break;
					case "2":
						operacja.deleteFromTable();
						break;
					case "3":
						operacja.usunwszystko();
						break;
					case "4":
						operacja.pobierzPoNazwie();
						break;
					case "5":
						operacja.pobierzZWarunkami();
						break;
					case "6":
						operacja.pobierzWszystko();
						break;
					case "7":
						operacja.przetworz();
						break;
					default:
						System.out.println("\u001B[31mbledny znak \u001B[37m");
				}
			}
			operacja.dropTable();
		}
	}
}